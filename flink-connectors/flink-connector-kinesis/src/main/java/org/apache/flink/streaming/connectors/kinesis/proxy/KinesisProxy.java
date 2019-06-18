/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kinesis.proxy;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.util.AWSUtil;
import org.apache.flink.streaming.connectors.kinesis.util.KinesisConfigUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.NonRetryableException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.ExpiredNextTokenException;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.InvalidArgumentException;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Kinesis proxy implementation - a utility class that is used as a proxy to make
 * calls to AWS Kinesis for several functions, such as getting a list of shards and
 * fetching a batch of data records starting from a specified record sequence number.
 *
 * <p>NOTE:
 * In the AWS KCL library, there is a similar implementation - {@link com.amazonaws.services.kinesis.clientlibrary.proxies.KinesisProxy}.
 * This implementation differs mainly in that we can make operations to arbitrary Kinesis streams, which is a needed
 * functionality for the Flink Kinesis Connector since the consumer may simultaneously read from multiple Kinesis streams.
 */
@Internal
public class KinesisProxy implements KinesisProxyInterface {

	private static final Logger LOG = LoggerFactory.getLogger(KinesisProxy.class);

	/** The actual Kinesis client from the AWS SDK that we will be using to make calls. */
	protected final KinesisClient kinesisClient;

	/** Random seed used to calculate backoff jitter for Kinesis operations. */
	private static final Random seed = new Random();

	// ------------------------------------------------------------------------
	//  listShards() related performance settings
	// ------------------------------------------------------------------------

	/** Base backoff millis for the list shards operation. */
	private final long listShardsBaseBackoffMillis;

	/** Maximum backoff millis for the list shards operation. */
	private final long listShardsMaxBackoffMillis;

	/** Exponential backoff power constant for the list shards operation. */
	private final double listShardsExpConstant;

	/** Maximum retry attempts for the list shards operation. */
	private final int listShardsMaxRetries;

	// ------------------------------------------------------------------------
	//  getRecords() related performance settings
	// ------------------------------------------------------------------------

	/** Base backoff millis for the get records operation. */
	private final long getRecordsBaseBackoffMillis;

	/** Maximum backoff millis for the get records operation. */
	private final long getRecordsMaxBackoffMillis;

	/** Exponential backoff power constant for the get records operation. */
	private final double getRecordsExpConstant;

	/** Maximum retry attempts for the get records operation. */
	private final int getRecordsMaxRetries;

	// ------------------------------------------------------------------------
	//  getShardIterator() related performance settings
	// ------------------------------------------------------------------------

	/** Base backoff millis for the get shard iterator operation. */
	private final long getShardIteratorBaseBackoffMillis;

	/** Maximum backoff millis for the get shard iterator operation. */
	private final long getShardIteratorMaxBackoffMillis;

	/** Exponential backoff power constant for the get shard iterator operation. */
	private final double getShardIteratorExpConstant;

	/** Maximum retry attempts for the get shard iterator operation. */
	private final int getShardIteratorMaxRetries;

	/* Backoff millis for the describe stream operation. */
	private final long describeStreamBaseBackoffMillis;

	/* Maximum backoff millis for the describe stream operation. */
	private final long describeStreamMaxBackoffMillis;

	/* Exponential backoff power constant for the describe stream operation. */
	private final double describeStreamExpConstant;

	/**
	 * Create a new KinesisProxy based on the supplied configuration properties.
	 *
	 * @param configProps configuration properties containing AWS credential and AWS region info
	 */
	protected KinesisProxy(Properties configProps) {
		checkNotNull(configProps);
		KinesisConfigUtil.backfillConsumerKeys(configProps);

		this.kinesisClient = createKinesisClient(configProps);

		this.listShardsBaseBackoffMillis = Long.valueOf(
			configProps.getProperty(
				ConsumerConfigConstants.LIST_SHARDS_BACKOFF_BASE,
				Long.toString(ConsumerConfigConstants.DEFAULT_LIST_SHARDS_BACKOFF_BASE)));
		this.listShardsMaxBackoffMillis = Long.valueOf(
			configProps.getProperty(
				ConsumerConfigConstants.LIST_SHARDS_BACKOFF_MAX,
				Long.toString(ConsumerConfigConstants.DEFAULT_LIST_SHARDS_BACKOFF_MAX)));
		this.listShardsExpConstant = Double.valueOf(
			configProps.getProperty(
				ConsumerConfigConstants.LIST_SHARDS_BACKOFF_EXPONENTIAL_CONSTANT,
				Double.toString(ConsumerConfigConstants.DEFAULT_LIST_SHARDS_BACKOFF_EXPONENTIAL_CONSTANT)));
		this.listShardsMaxRetries = Integer.valueOf(
			configProps.getProperty(
				ConsumerConfigConstants.LIST_SHARDS_RETRIES,
				Long.toString(ConsumerConfigConstants.DEFAULT_LIST_SHARDS_RETRIES)));
		this.describeStreamBaseBackoffMillis = Long.valueOf(
				configProps.getProperty(ConsumerConfigConstants.STREAM_DESCRIBE_BACKOFF_BASE,
						Long.toString(ConsumerConfigConstants.DEFAULT_STREAM_DESCRIBE_BACKOFF_BASE)));
		this.describeStreamMaxBackoffMillis = Long.valueOf(
				configProps.getProperty(ConsumerConfigConstants.STREAM_DESCRIBE_BACKOFF_MAX,
						Long.toString(ConsumerConfigConstants.DEFAULT_STREAM_DESCRIBE_BACKOFF_MAX)));
		this.describeStreamExpConstant = Double.valueOf(
				configProps.getProperty(ConsumerConfigConstants.STREAM_DESCRIBE_BACKOFF_EXPONENTIAL_CONSTANT,
						Double.toString(ConsumerConfigConstants.DEFAULT_STREAM_DESCRIBE_BACKOFF_EXPONENTIAL_CONSTANT)));
		this.getRecordsBaseBackoffMillis = Long.valueOf(
			configProps.getProperty(
				ConsumerConfigConstants.SHARD_GETRECORDS_BACKOFF_BASE,
				Long.toString(ConsumerConfigConstants.DEFAULT_SHARD_GETRECORDS_BACKOFF_BASE)));
		this.getRecordsMaxBackoffMillis = Long.valueOf(
			configProps.getProperty(
				ConsumerConfigConstants.SHARD_GETRECORDS_BACKOFF_MAX,
				Long.toString(ConsumerConfigConstants.DEFAULT_SHARD_GETRECORDS_BACKOFF_MAX)));
		this.getRecordsExpConstant = Double.valueOf(
			configProps.getProperty(
				ConsumerConfigConstants.SHARD_GETRECORDS_BACKOFF_EXPONENTIAL_CONSTANT,
				Double.toString(ConsumerConfigConstants.DEFAULT_SHARD_GETRECORDS_BACKOFF_EXPONENTIAL_CONSTANT)));
		this.getRecordsMaxRetries = Integer.valueOf(
			configProps.getProperty(
				ConsumerConfigConstants.SHARD_GETRECORDS_RETRIES,
				Long.toString(ConsumerConfigConstants.DEFAULT_SHARD_GETRECORDS_RETRIES)));

		this.getShardIteratorBaseBackoffMillis = Long.valueOf(
			configProps.getProperty(
				ConsumerConfigConstants.SHARD_GETITERATOR_BACKOFF_BASE,
				Long.toString(ConsumerConfigConstants.DEFAULT_SHARD_GETITERATOR_BACKOFF_BASE)));
		this.getShardIteratorMaxBackoffMillis = Long.valueOf(
			configProps.getProperty(
				ConsumerConfigConstants.SHARD_GETITERATOR_BACKOFF_MAX,
				Long.toString(ConsumerConfigConstants.DEFAULT_SHARD_GETITERATOR_BACKOFF_MAX)));
		this.getShardIteratorExpConstant = Double.valueOf(
			configProps.getProperty(
				ConsumerConfigConstants.SHARD_GETITERATOR_BACKOFF_EXPONENTIAL_CONSTANT,
				Double.toString(ConsumerConfigConstants.DEFAULT_SHARD_GETITERATOR_BACKOFF_EXPONENTIAL_CONSTANT)));
		this.getShardIteratorMaxRetries = Integer.valueOf(
			configProps.getProperty(
				ConsumerConfigConstants.SHARD_GETITERATOR_RETRIES,
				Long.toString(ConsumerConfigConstants.DEFAULT_SHARD_GETITERATOR_RETRIES)));

	}

	/**
	 * Create the Kinesis client, using the provided configuration properties and default {@link ClientOverrideConfiguration}.
	 * Derived classes can override this method to customize the client configuration.
	 * @param configProps
	 * @return
	 */
	protected KinesisClient createKinesisClient(Properties configProps) {

		ClientOverrideConfiguration.Builder awsClientConfig = ClientOverrideConfiguration.builder();
		AWSUtil.setAwsClientOverrideConfigurationProperties(awsClientConfig, configProps);
		SdkHttpClient.Builder sdkHttpClient = ApacheHttpClient.builder();
		AWSUtil.setSdkHttpClientConfigurationProperties(sdkHttpClient, configProps);
		return AWSUtil.createKinesisClient(configProps, awsClientConfig, sdkHttpClient.build());
	}

	/**
	 * Creates a Kinesis proxy.
	 *
	 * @param configProps configuration properties
	 * @return the created kinesis proxy
	 */
	public static KinesisProxyInterface create(Properties configProps) {
		return new KinesisProxy(configProps);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public GetRecordsResponse getRecords(String shardIterator, int maxRecordsToGet) throws InterruptedException {
		final GetRecordsRequest getRecordsRequest = GetRecordsRequest.builder()
			.shardIterator(shardIterator)
			.limit(maxRecordsToGet)
			.build();

		GetRecordsResponse getRecordsResponse = null;

		int retryCount = 0;
		while (retryCount <= getRecordsMaxRetries && getRecordsResponse == null) {
			try {
				getRecordsResponse = kinesisClient.getRecords(getRecordsRequest);
			} catch (software.amazon.awssdk.core.exception.SdkClientException ex) {
				if (isRecoverableSdkClientException(ex)) {
					long backoffMillis = fullJitterBackoff(
						getRecordsBaseBackoffMillis, getRecordsMaxBackoffMillis, getRecordsExpConstant, retryCount++);
					LOG.warn("Got recoverable SdkClientException. Backing off for "
						+ backoffMillis + " millis (" + ex.getClass().getName() + ": " + ex.getMessage() + ")");
					Thread.sleep(backoffMillis);
				} else {
					throw ex;
				}
			}
		}

		if (getRecordsResponse == null) {
			throw new RuntimeException("Retries exceeded for getRecords operation - all " + getRecordsMaxRetries +
				" retry attempts failed.");
		}

		return getRecordsResponse;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public GetShardListResult getShardList(Map<String, String> streamNamesWithLastSeenShardIds) throws InterruptedException {
		GetShardListResult result = new GetShardListResult();

		for (Map.Entry<String, String> streamNameWithLastSeenShardId : streamNamesWithLastSeenShardIds.entrySet()) {
			String stream = streamNameWithLastSeenShardId.getKey();
			String lastSeenShardId = streamNameWithLastSeenShardId.getValue();
			result.addRetrievedShardsToStream(stream, getShardsOfStream(stream, lastSeenShardId));
		}
		return result;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getShardIterator(StreamShardHandle shard, String shardIteratorType, @Nullable Object startingMarker) throws InterruptedException {
		GetShardIteratorRequest.Builder getShardIteratorRequest = GetShardIteratorRequest.builder()
			.streamName(shard.getStreamName())
			.shardId(shard.getShard().shardId())
			.shardIteratorType(shardIteratorType);

		switch (ShardIteratorType.fromValue(shardIteratorType)) {
			case TRIM_HORIZON:
			case LATEST:
				break;
			case AT_TIMESTAMP:
				if (startingMarker instanceof Instant) {
					getShardIteratorRequest.timestamp((Instant) startingMarker);
				} else if (startingMarker instanceof Date) {
					getShardIteratorRequest.timestamp(((Date) startingMarker).toInstant());
				} else {
					throw new IllegalArgumentException("Invalid object given for GetShardIteratorRequest() when ShardIteratorType is AT_TIMESTAMP. Must be a Date object.");
				}
				break;
			case AT_SEQUENCE_NUMBER:
			case AFTER_SEQUENCE_NUMBER:
				if (startingMarker instanceof String) {
					getShardIteratorRequest.startingSequenceNumber((String) startingMarker);
				} else {
					throw new IllegalArgumentException("Invalid object given for GetShardIteratorRequest() when ShardIteratorType is AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER. Must be a String.");
				}
		}
		return getShardIterator(getShardIteratorRequest.build());
	}

	private String getShardIterator(GetShardIteratorRequest getShardIteratorRequest) throws InterruptedException {
		GetShardIteratorResponse getShardIteratorResponse = null;

		int retryCount = 0;
		while (retryCount <= getShardIteratorMaxRetries && getShardIteratorResponse == null) {
			try {
				getShardIteratorResponse = kinesisClient.getShardIterator(getShardIteratorRequest);
			} catch (SdkServiceException ex) {
				long backoffMillis = fullJitterBackoff(
					getShardIteratorBaseBackoffMillis, getShardIteratorMaxBackoffMillis, getShardIteratorExpConstant, retryCount++);
				LOG.warn("Got recoverable SdkServiceException. Backing off for "
					+ backoffMillis + " millis (" + ex.getClass().getName() + ": " + ex.getMessage() + ")");
				Thread.sleep(backoffMillis);
			}
		}

		if (getShardIteratorResponse == null) {
			throw new RuntimeException("Retries exceeded for getShardIterator operation - all " + getShardIteratorMaxRetries +
				" retry attempts failed.");
		}
		return getShardIteratorResponse.shardIterator();
	}

	/**
	 * Determines whether the exception is recoverable using exponential-backoff.
	 *
	 * @param ex Exception to inspect
	 * @return <code>true</code> if the exception can be recovered from, else
	 *         <code>false</code>
	 */
	protected boolean isRecoverableSdkClientException(SdkClientException ex) {
		return !(ex instanceof NonRetryableException);
	}

	private List<StreamShardHandle> getShardsOfStream(String streamName, @Nullable String lastSeenShardId) throws InterruptedException {
		List<StreamShardHandle> shardsOfStream = new ArrayList<>();

		// List Shards returns just the first 1000 shard entries. In order to read the entire stream,
		// we need to use the returned nextToken to get additional shards.
		ListShardsResponse listShardsResponse;
		String startShardToken = null;
		do {
			listShardsResponse = listShards(streamName, lastSeenShardId, startShardToken);
			if (listShardsResponse == null) {
				// In case we have exceptions while retrieving all shards, ensure that incomplete shard list is not returned.
				// Hence clearing the incomplete shard list before returning it.
				shardsOfStream.clear();
				return shardsOfStream;
			}
			List<Shard> shards = listShardsResponse.shards();
			for (Shard shard : shards) {
				shardsOfStream.add(new StreamShardHandle(streamName, shard));
			}
			startShardToken = listShardsResponse.nextToken();
		} while (startShardToken != null);

		return shardsOfStream;
	}

	/**
	 * Get metainfo for a Kinesis stream, which contains information about which shards this Kinesis stream possess.
	 *
	 * <p>This method is using a "full jitter" approach described in AWS's article,
	 * <a href="https://www.awsarchitectureblog.com/2015/03/backoff.html">"Exponential Backoff and Jitter"</a>.
	 * This is necessary because concurrent calls will be made by all parallel subtask's fetcher. This
	 * jitter backoff approach will help distribute calls across the fetchers over time.
	 *
	 * @param streamName the stream to describe
	 * @param startShardId which shard to start with for this describe operation (earlier shard's infos will not appear in result)
	 * @return the result of the describe stream operation
	 */
	private ListShardsResponse listShards(String streamName, @Nullable String startShardId,
											@Nullable String startNextToken)
		throws InterruptedException {
		final ListShardsRequest.Builder listShardsRequest = ListShardsRequest.builder();
		if (startNextToken == null) {
			listShardsRequest.exclusiveStartShardId(startShardId);
			listShardsRequest.streamName(streamName);
		} else {
			// Note the nextToken returned by AWS expires within 300 sec.
			listShardsRequest.nextToken(startNextToken);
		}

		ListShardsResponse listShardsResults = null;

		// Call ListShards, with full-jitter backoff (if we get LimitExceededException).
		int retryCount = 0;
		// List Shards returns just the first 1000 shard entries. Make sure that all entries
		// are taken up.
		while (retryCount <= listShardsMaxRetries && listShardsResults == null) { // retry until we get a result
			try {

				listShardsResults = kinesisClient.listShards(listShardsRequest.build());
			} catch (LimitExceededException le) {
				long backoffMillis = fullJitterBackoff(
					listShardsBaseBackoffMillis, listShardsMaxBackoffMillis, listShardsExpConstant, retryCount++);
				LOG.warn("Got LimitExceededException when listing shards from stream " + streamName
					+ ". Backing off for " + backoffMillis + " millis.");
				Thread.sleep(backoffMillis);
			} catch (ResourceInUseException reInUse) {
				if (LOG.isWarnEnabled()) {
					// List Shards will throw an exception if stream in not in active state. Return and re-use previous state available.
					LOG.info("The stream is currently not in active state. Reusing the older state "
						+ "for the time being");
					break;
				}
			} catch (ResourceNotFoundException reNotFound) {
				throw new RuntimeException("Stream not found. Error while getting shard list.", reNotFound);
			} catch (InvalidArgumentException inArg) {
				throw new RuntimeException("Invalid Arguments to listShards.", inArg);
			} catch (ExpiredNextTokenException expiredToken) {
				LOG.warn("List Shards has an expired token. Reusing the previous state.");
				break;
			} catch (software.amazon.awssdk.core.exception.SdkClientException ex) {
				if (retryCount < listShardsMaxRetries && isRecoverableSdkClientException(ex)) {
					long backoffMillis = fullJitterBackoff(
						listShardsBaseBackoffMillis, listShardsMaxBackoffMillis, listShardsExpConstant, retryCount++);
					LOG.warn("Got SdkClientException when listing shards from stream {}. Backing off for {} millis.",
						streamName, backoffMillis);
					Thread.sleep(backoffMillis);
				} else {
					// propagate if retries exceeded or not recoverable
					// (otherwise would return null result and keep trying forever)
					throw ex;
				}
			} catch (AwsServiceException serviceEx) {
				//TODO: note that previously all service exceptions were treated as retryable.
				// and also that the one other retryable, ProvisionedThroughputExceededException is now a service exception, not a client exception
				if (retryCount < listShardsMaxRetries) {
					long backoffMillis = fullJitterBackoff(
						listShardsBaseBackoffMillis, listShardsMaxBackoffMillis, listShardsExpConstant, retryCount++);
					LOG.warn("Got AwsServiceException when listing shards from stream {}. Backing off for {} millis.",
						streamName, backoffMillis);
					Thread.sleep(backoffMillis);
				} else {
					// propagate if retries exceeded or not recoverable
					// (otherwise would return null result and keep trying forever)
					throw serviceEx;
				}

			}
		}

		// Kinesalite (mock implementation of Kinesis) does not correctly exclude shards before
		// the exclusive start shard id in the returned shards list; check if we need to remove
		// these erroneously returned shards.
		// Related issues:
		// 	https://github.com/mhart/kinesalite/pull/77
		// 	https://github.com/lyft/kinesalite/pull/4
		if (startShardId != null && listShardsResults != null) {
			List<Shard> shards = listShardsResults.shards();
			Iterator<Shard> shardItr = shards.iterator();
			while (shardItr.hasNext()) {
				if (StreamShardHandle.compareShardIds(shardItr.next().shardId(), startShardId) <= 0) {
					shardItr.remove();
				}
			}
		}

		return listShardsResults;
	}

	/**
	 * Get metainfo for a Kinesis stream, which contains information about which shards this
	 * Kinesis stream possess.
	 *
	 * <p>This method is using a "full jitter" approach described in AWS's article,
	 * <a href="https://www.awsarchitectureblog.com/2015/03/backoff.html">
	 *   "Exponential Backoff and Jitter"</a>.
	 * This is necessary because concurrent calls will be made by all parallel subtask's fetcher.
	 * This jitter backoff approach will help distribute calls across the fetchers over time.
	 *
	 * @param streamName the stream to describe
	 * @param startShardId which shard to start with for this describe operation
	 *
	 * @return the result of the describe stream operation
	 */
	protected DescribeStreamResponse describeStream(String streamName, @Nullable String startShardId)
		throws InterruptedException {
		final DescribeStreamRequest describeStreamRequest = DescribeStreamRequest.builder()
			.streamName(streamName)
			.exclusiveStartShardId(startShardId)
			.build();

		DescribeStreamResponse describeStreamResponse = null;

		// Call DescribeStream, with full-jitter backoff (if we get LimitExceededException).
		int attemptCount = 0;
		while (describeStreamResponse == null) { // retry until we get a result
			try {
				describeStreamResponse = kinesisClient.describeStream(describeStreamRequest);
			} catch (LimitExceededException le) {
				long backoffMillis = fullJitterBackoff(
					describeStreamBaseBackoffMillis,
					describeStreamMaxBackoffMillis,
					describeStreamExpConstant,
					attemptCount++);
				LOG.warn(String.format("Got LimitExceededException when describing stream %s. "
					+ "Backing off for %d millis.", streamName, backoffMillis));
				Thread.sleep(backoffMillis);
			} catch (ResourceNotFoundException re) {
				throw new RuntimeException("Error while getting stream details", re);
			}
		}

		StreamStatus streamStatus = describeStreamResponse.streamDescription().streamStatus();
		if (!(streamStatus == StreamStatus.ACTIVE || streamStatus == StreamStatus.UPDATING)) {
			if (LOG.isWarnEnabled()) {
				LOG.warn(String.format("The status of stream %s is %s ; result of the current "
						+ "describeStream operation will not contain any shard information.",
					streamName, streamStatus));
			}
		}

		return describeStreamResponse;
	}

	protected static long fullJitterBackoff(long base, long max, double power, int attempt) {
		long exponentialBackoff = (long) Math.min(max, base * Math.pow(power, attempt));
		return (long) (seed.nextDouble() * exponentialBackoff); // random jitter between 0 and the exponential backoff
	}
}
