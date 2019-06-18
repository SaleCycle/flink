package org.apache.flink.streaming.connectors.kinesis.proxy;

import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.exception.NonRetryableException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.dynamodb.model.LimitExceededException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType;
import software.amazon.awssdk.services.dynamodb.model.StreamStatus;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClientBuilder;

import javax.annotation.Nullable;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.AWS_ENDPOINT;
import static org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.AWS_REGION;
import static org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxy.fullJitterBackoff;
import static org.apache.flink.streaming.connectors.kinesis.util.AWSUtil.getAwsCredentialsProvider;
import static org.apache.flink.streaming.connectors.kinesis.util.AWSUtil.setAwsClientOverrideConfigurationProperties;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * DynamoDB streams proxy: interface interacting with the DynamoDB streams.
 */
public class DynamoDBStreamsProxy implements KinesisProxyInterface	{
	private static final Logger LOG = LoggerFactory.getLogger(DynamoDBStreamsProxy.class);

	/** Used for formatting Flink-specific user agent string when creating Kinesis client. */
	private static final String USER_AGENT_FORMAT = "Apache Flink %s (%s) DynamoDB Streams Connector";

	private final DynamoDbStreamsClient streamsClient;


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

	protected DynamoDBStreamsProxy(Properties configProps) {
		checkNotNull(configProps);
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

		this.streamsClient = createDynamoDbStreamsClient(configProps);
	}

	/**
	 * Creates a DynamoDB streams proxy.
	 *
	 * @param configProps configuration properties
	 * @return the created DynamoDB streams proxy
	 */
	public static KinesisProxyInterface create(Properties configProps) {
		return new DynamoDBStreamsProxy(configProps);
	}

	/**
	 * Creates an AmazonDynamoDBStreamsAdapterClient.
	 * Uses it as the internal client interacting with the DynamoDB streams.
	 *
	 * @param configProps configuration properties
	 * @return an AWS DynamoDB streams adapter client
	 */
	protected DynamoDbStreamsClient createDynamoDbStreamsClient(Properties configProps) {
		ClientOverrideConfiguration.Builder awsClientConfig = ClientOverrideConfiguration.builder();
		setAwsClientOverrideConfigurationProperties(awsClientConfig, configProps);

		AwsCredentialsProvider credentials = getAwsCredentialsProvider(configProps);
		awsClientConfig.putAdvancedOption(SdkAdvancedClientOption.USER_AGENT_PREFIX,
			String.format(
				USER_AGENT_FORMAT,
				EnvironmentInformation.getVersion(),
				EnvironmentInformation.getRevisionInformation().commitId));

		DynamoDbStreamsClientBuilder adapterClient = DynamoDbStreamsClient.builder()
			.credentialsProvider(credentials)
			.overrideConfiguration(awsClientConfig.build());

		if (configProps.containsKey(AWS_ENDPOINT)) {
			try {
				adapterClient.endpointOverride(new URI(configProps.getProperty(AWS_ENDPOINT)));
			} catch (URISyntaxException e) {
				throw new RuntimeException(e);
			}
		} else {
			adapterClient.region(Region.of(configProps.getProperty(AWS_REGION)));
		}

		return adapterClient.build();
	}

	@Override
	public String getShardIterator(StreamShardHandle shard, String shardIteratorType, Object startingMarker) throws InterruptedException {
		GetShardIteratorRequest.Builder getShardIteratorRequest = GetShardIteratorRequest.builder()
			.streamArn(shard.getStreamName())
			.shardId(shard.getShard().shardId())
			.shardIteratorType(shardIteratorType);

		switch (ShardIteratorType.fromValue(shardIteratorType)) {
			case TRIM_HORIZON:
			case LATEST:
				break;
			case AT_SEQUENCE_NUMBER:
			case AFTER_SEQUENCE_NUMBER:
				if (startingMarker instanceof String) {
					getShardIteratorRequest.sequenceNumber((String) startingMarker);
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
				getShardIteratorResponse = streamsClient.getShardIterator(getShardIteratorRequest);
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
	 * {@inheritDoc}
	 */
	@Override
	public software.amazon.awssdk.services.kinesis.model.GetRecordsResponse getRecords(String shardIterator, int maxRecordsToGet) throws InterruptedException {
		final GetRecordsRequest getRecordsRequest = GetRecordsRequest.builder()
			.shardIterator(shardIterator)
			.limit(maxRecordsToGet)
			.build();

		GetRecordsResponse getRecordsResponse = null;

		int retryCount = 0;
		while (retryCount <= getRecordsMaxRetries && getRecordsResponse == null) {
			try {
				getRecordsResponse = streamsClient.getRecords(getRecordsRequest);
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

		software.amazon.awssdk.services.kinesis.model.GetRecordsResponse kGetRecordsResponse = software.amazon.awssdk.services.kinesis.model.GetRecordsResponse
			.builder()
			.records(getRecordsResponse.records().stream().map(r -> software.amazon.awssdk.services.kinesis.model.Record.builder()
				.approximateArrivalTimestamp(r.dynamodb().approximateCreationDateTime())
				// TODO: this mapping from a DynamoDB record to a Kinesis Record. Possibly the entire thing can be serialised and set as data?
				.data(SdkBytes.fromUtf8String("no idea"))
				.sequenceNumber(r.dynamodb().sequenceNumber())
				.build()).collect(Collectors.toList()))
			.nextShardIterator(getRecordsResponse.nextShardIterator())
			.build();
		return kGetRecordsResponse;
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

	@Override
	public GetShardListResult getShardList(
		Map<String, String> streamNamesWithLastSeenShardIds) throws InterruptedException {
		GetShardListResult result = new GetShardListResult();

		for (Map.Entry<String, String> streamNameWithLastSeenShardId :
			streamNamesWithLastSeenShardIds.entrySet()) {
			String stream = streamNameWithLastSeenShardId.getKey();
			String lastSeenShardId = streamNameWithLastSeenShardId.getValue();
			result.addRetrievedShardsToStream(stream, getShardsOfStream(stream, lastSeenShardId));
		}
		return result;
	}

	private List<StreamShardHandle> getShardsOfStream(
		String streamName,
		@Nullable String lastSeenShardId)
		throws InterruptedException {
		List<StreamShardHandle> shardsOfStream = new ArrayList<>();

		DescribeStreamResponse describeStreamResult;
		describeStreamResult = describeStream(streamName, lastSeenShardId);
		List<Shard> shards = describeStreamResult.streamDescription().shards();
		for (Shard shard : shards) {
			software.amazon.awssdk.services.kinesis.model.Shard kShard = software.amazon.awssdk.services.kinesis.model.Shard.builder()
				.sequenceNumberRange(software.amazon.awssdk.services.kinesis.model.SequenceNumberRange.builder()
					.startingSequenceNumber(shard.sequenceNumberRange().startingSequenceNumber())
					.endingSequenceNumber(shard.sequenceNumberRange().endingSequenceNumber())
					.build())
				.shardId(shard.shardId())
				.parentShardId(shard.parentShardId())
				.build();
			shardsOfStream.add(new StreamShardHandle(streamName, kShard));
		}

		if (shards.size() != 0) {
			lastSeenShardId = shards.get(shards.size() - 1).shardId();
		}

		return shardsOfStream;
	}

	protected DescribeStreamResponse describeStream(String streamName, @Nullable String startShardId)
		throws InterruptedException {
		final DescribeStreamRequest describeStreamRequest = DescribeStreamRequest.builder()
			.streamArn(streamName)
			.exclusiveStartShardId(startShardId)
			.build();

		DescribeStreamResponse describeStreamResponse = null;

		// Call DescribeStream, with full-jitter backoff (if we get LimitExceededException).
		int attemptCount = 0;
		while (describeStreamResponse == null) { // retry until we get a result
			try {
				describeStreamResponse = streamsClient.describeStream(describeStreamRequest);
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
		if (!(streamStatus == StreamStatus.ENABLED || streamStatus == StreamStatus.ENABLING)) {
			if (LOG.isWarnEnabled()) {
				LOG.warn(String.format("The status of stream %s is %s ; result of the current "
						+ "describeStream operation will not contain any shard information.",
					streamName, streamStatus));
			}
		}

		return describeStreamResponse;
	}
}
