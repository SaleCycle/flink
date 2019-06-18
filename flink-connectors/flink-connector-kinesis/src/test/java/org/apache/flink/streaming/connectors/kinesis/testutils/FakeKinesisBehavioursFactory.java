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

package org.apache.flink.streaming.connectors.kinesis.testutils;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.proxy.GetShardListResult;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyInterface;
import org.apache.flink.util.Preconditions;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.HashKeyRange;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.SequenceNumberRange;
import software.amazon.awssdk.services.kinesis.model.Shard;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Factory for different kinds of fake Kinesis behaviours using the {@link KinesisProxyInterface} interface.
 */
public class FakeKinesisBehavioursFactory {

	// ------------------------------------------------------------------------
	//  Behaviours related to shard listing and resharding, used in KinesisDataFetcherTest
	// ------------------------------------------------------------------------

	public static KinesisProxyInterface noShardsFoundForRequestedStreamsBehaviour() {

		return new KinesisProxyInterface() {
			@Override
			public GetShardListResult getShardList(Map<String, String> streamNamesWithLastSeenShardIds) {
				return new GetShardListResult(); // not setting any retrieved shards for result
			}

			@Override
			public String getShardIterator(StreamShardHandle shard, String shardIteratorType, Object startingMarker) {
				return null;
			}

			@Override
			public GetRecordsResponse getRecords(String shardIterator, int maxRecordsToGet) {
				return null;
			}
		};

	}

	public static KinesisProxyInterface nonReshardedStreamsBehaviour(Map<String, Integer> streamsToShardCount) {
		return new NonReshardedStreamsKinesis(streamsToShardCount);

	}

	// ------------------------------------------------------------------------
	//  Behaviours related to fetching records, used mainly in ShardConsumerTest
	// ------------------------------------------------------------------------

	public static KinesisProxyInterface totalNumOfRecordsAfterNumOfGetRecordsCalls(
			final int numOfRecords,
			final int numOfGetRecordsCalls,
			final long millisBehindLatest) {
		return new SingleShardEmittingFixNumOfRecordsKinesis(numOfRecords, numOfGetRecordsCalls, millisBehindLatest);
	}

	public static KinesisProxyInterface totalNumOfRecordsAfterNumOfGetRecordsCallsWithUnexpectedExpiredIterator(
			final int numOfRecords,
			final int numOfGetRecordsCall,
			final int orderOfCallToExpire,
			final long millisBehindLatest) {
		return new SingleShardEmittingFixNumOfRecordsWithExpiredIteratorKinesis(
			numOfRecords, numOfGetRecordsCall, orderOfCallToExpire, millisBehindLatest);
	}

	public static KinesisProxyInterface initialNumOfRecordsAfterNumOfGetRecordsCallsWithAdaptiveReads(
			final int numOfRecords,
			final int numOfGetRecordsCalls,
			final long millisBehindLatest) {
		return new SingleShardEmittingAdaptiveNumOfRecordsKinesis(numOfRecords, numOfGetRecordsCalls,
				millisBehindLatest);
	}

	public static KinesisProxyInterface blockingQueueGetRecords(Map<String, List<BlockingQueue<String>>> streamsToShardQueues) {
		return new BlockingQueueKinesis(streamsToShardQueues);
	}

	private static class SingleShardEmittingFixNumOfRecordsWithExpiredIteratorKinesis extends SingleShardEmittingFixNumOfRecordsKinesis {

		private final long millisBehindLatest;
		private final int orderOfCallToExpire;

		private boolean expiredOnceAlready = false;
		private boolean expiredIteratorRefreshed = false;

		public SingleShardEmittingFixNumOfRecordsWithExpiredIteratorKinesis(
				final int numOfRecords,
				final int numOfGetRecordsCalls,
				final int orderOfCallToExpire,
				final long millisBehindLatest) {
			super(numOfRecords, numOfGetRecordsCalls, millisBehindLatest);
			checkArgument(orderOfCallToExpire <= numOfGetRecordsCalls,
				"can not test unexpected expired iterator if orderOfCallToExpire is larger than numOfGetRecordsCalls");
			this.millisBehindLatest = millisBehindLatest;
			this.orderOfCallToExpire = orderOfCallToExpire;
		}

		@Override
		public GetRecordsResponse getRecords(String shardIterator, int maxRecordsToGet) {
			if ((Integer.valueOf(shardIterator) == orderOfCallToExpire - 1) && !expiredOnceAlready) {
				// we fake only once the expired iterator exception at the specified get records attempt order
				expiredOnceAlready = true;
				throw ExpiredIteratorException.builder().message("Artificial expired shard iterator").build();
			} else if (expiredOnceAlready && !expiredIteratorRefreshed) {
				// if we've thrown the expired iterator exception already, but the iterator was not refreshed,
				// throw a hard exception to the test that is testing this Kinesis behaviour
				throw new RuntimeException("expired shard iterator was not refreshed on the next getRecords() call");
			} else {
				// assuming that the maxRecordsToGet is always large enough
				return GetRecordsResponse.builder()
					.records(shardItrToRecordBatch.get(shardIterator))
					.millisBehindLatest(millisBehindLatest)
					.nextShardIterator(
						(Integer.valueOf(shardIterator) == totalNumOfGetRecordsCalls - 1)
							? null : String.valueOf(Integer.valueOf(shardIterator) + 1)) // last next shard iterator is null
					.build();
			}
		}

		@Override
		public String getShardIterator(StreamShardHandle shard, String shardIteratorType, Object startingMarker) {
			if (!expiredOnceAlready) {
				// for the first call, just return the iterator of the first batch of records
				return "0";
			} else {
				// fake the iterator refresh when this is called again after getRecords throws expired iterator
				// exception on the orderOfCallToExpire attempt
				expiredIteratorRefreshed = true;
				return String.valueOf(orderOfCallToExpire - 1);
			}
		}
	}

	private static class SingleShardEmittingFixNumOfRecordsKinesis implements KinesisProxyInterface {

		protected final int totalNumOfGetRecordsCalls;

		protected final int totalNumOfRecords;

		private final long millisBehindLatest;

		protected final Map<String, List<Record>> shardItrToRecordBatch;

		public SingleShardEmittingFixNumOfRecordsKinesis(
				final int numOfRecords,
				final int numOfGetRecordsCalls,
				final long millistBehindLatest) {
			this.totalNumOfRecords = numOfRecords;
			this.totalNumOfGetRecordsCalls = numOfGetRecordsCalls;
			this.millisBehindLatest = millistBehindLatest;

			// initialize the record batches that we will be fetched
			this.shardItrToRecordBatch = new HashMap<>();

			int numOfAlreadyPartitionedRecords = 0;
			int numOfRecordsPerBatch = numOfRecords / numOfGetRecordsCalls + 1;
			for (int batch = 0; batch < totalNumOfGetRecordsCalls; batch++) {
				if (batch != totalNumOfGetRecordsCalls - 1) {
					shardItrToRecordBatch.put(
						String.valueOf(batch),
						createRecordBatchWithRange(
							numOfAlreadyPartitionedRecords,
							numOfAlreadyPartitionedRecords + numOfRecordsPerBatch));
					numOfAlreadyPartitionedRecords += numOfRecordsPerBatch;
				} else {
					shardItrToRecordBatch.put(
						String.valueOf(batch),
						createRecordBatchWithRange(
							numOfAlreadyPartitionedRecords,
							totalNumOfRecords));
				}
			}
		}

		@Override
		public GetRecordsResponse getRecords(String shardIterator, int maxRecordsToGet) {
			// assuming that the maxRecordsToGet is always large enough
			return GetRecordsResponse.builder()
				.records(shardItrToRecordBatch.get(shardIterator))
				.millisBehindLatest(millisBehindLatest)
				.nextShardIterator(
					(Integer.valueOf(shardIterator) == totalNumOfGetRecordsCalls - 1)
						? null : String.valueOf(Integer.valueOf(shardIterator) + 1)) // last next shard iterator is null
				.build();
		}

		@Override
		public String getShardIterator(StreamShardHandle shard, String shardIteratorType, Object startingMarker) {
			// this will be called only one time per ShardConsumer;
			// so, simply return the iterator of the first batch of records
			return "0";
		}

		@Override
		public GetShardListResult getShardList(Map<String, String> streamNamesWithLastSeenShardIds) {
			return null;
		}

		public static List<Record> createRecordBatchWithRange(int min, int max) {
			List<Record> batch = new LinkedList<>();
			for (int i = min; i < max; i++) {
				batch.add(
					Record.builder()
						.data(SdkBytes.fromString(String.valueOf(i), ConfigConstants.DEFAULT_CHARSET))
						.partitionKey(UUID.randomUUID().toString())
						.approximateArrivalTimestamp(Instant.now())
						.sequenceNumber(String.valueOf(i))
						.build());
			}
			return batch;
		}

	}

	private static class SingleShardEmittingAdaptiveNumOfRecordsKinesis implements
			KinesisProxyInterface {

		protected final int totalNumOfGetRecordsCalls;

		protected final int totalNumOfRecords;

		private final long millisBehindLatest;

		protected final Map<String, List<Record>> shardItrToRecordBatch;

		protected static long averageRecordSizeBytes;

		private static final long KINESIS_SHARD_BYTES_PER_SECOND_LIMIT = 2 * 1024L * 1024L;

		public SingleShardEmittingAdaptiveNumOfRecordsKinesis(final int numOfRecords,
				final int numOfGetRecordsCalls,
				final long millisBehindLatest) {
			this.totalNumOfRecords = numOfRecords;
			this.totalNumOfGetRecordsCalls = numOfGetRecordsCalls;
			this.millisBehindLatest = millisBehindLatest;
			this.averageRecordSizeBytes = 0L;

			// initialize the record batches that we will be fetched
			this.shardItrToRecordBatch = new HashMap<>();

			int numOfAlreadyPartitionedRecords = 0;
			int numOfRecordsPerBatch = numOfRecords;
			for (int batch = 0; batch < totalNumOfGetRecordsCalls; batch++) {
					shardItrToRecordBatch.put(
							String.valueOf(batch),
							createRecordBatchWithRange(
									numOfAlreadyPartitionedRecords,
									numOfAlreadyPartitionedRecords + numOfRecordsPerBatch));
					numOfAlreadyPartitionedRecords += numOfRecordsPerBatch;

				numOfRecordsPerBatch = (int) (KINESIS_SHARD_BYTES_PER_SECOND_LIMIT /
						(averageRecordSizeBytes * 1000L / ConsumerConfigConstants.DEFAULT_SHARD_GETRECORDS_INTERVAL_MILLIS));
			}
		}

		@Override
		public GetRecordsResponse getRecords(String shardIterator, int maxRecordsToGet) {
			// assuming that the maxRecordsToGet is always large enough
			return GetRecordsResponse.builder()
					.records(shardItrToRecordBatch.get(shardIterator))
					.millisBehindLatest(millisBehindLatest)
					.nextShardIterator(
							(Integer.valueOf(shardIterator) == totalNumOfGetRecordsCalls - 1)
									? null : String
									.valueOf(Integer.valueOf(shardIterator) + 1)) // last next shard iterator is null
					.build();
		}

		@Override
		public String getShardIterator(StreamShardHandle shard, String shardIteratorType,
				Object startingMarker) {
			// this will be called only one time per ShardConsumer;
			// so, simply return the iterator of the first batch of records
			return "0";
		}

		@Override
		public GetShardListResult getShardList(Map<String, String> streamNamesWithLastSeenShardIds) {
			return null;
		}

		public static List<Record> createRecordBatchWithRange(int min, int max) {
			List<Record> batch = new LinkedList<>();
			long	sumRecordBatchBytes = 0L;
			// Create record of size 10Kb
			String data = createDataSize(10 * 1024L);

			for (int i = min; i < max; i++) {
				Record record = Record.builder()
								.data(SdkBytes.fromString(data, ConfigConstants.DEFAULT_CHARSET))
								.partitionKey(UUID.randomUUID().toString())
								.approximateArrivalTimestamp(Instant.now())
								.sequenceNumber(String.valueOf(i))
								.build();
				batch.add(record);
				sumRecordBatchBytes += record.data().asByteBuffer().remaining();

			}
			if (batch.size() != 0) {
				averageRecordSizeBytes = sumRecordBatchBytes / batch.size();
			}

			return batch;
		}

		private static String createDataSize(long msgSize) {
			char[] data = new char[(int) msgSize];
			return new String(data);

		}

	}

	private static class NonReshardedStreamsKinesis implements KinesisProxyInterface {

		private Map<String, List<StreamShardHandle>> streamsWithListOfShards = new HashMap<>();

		public NonReshardedStreamsKinesis(Map<String, Integer> streamsToShardCount) {
			for (Map.Entry<String, Integer> streamToShardCount : streamsToShardCount.entrySet()) {
				String streamName = streamToShardCount.getKey();
				int shardCount = streamToShardCount.getValue();

				if (shardCount == 0) {
					// don't do anything
				} else {
					List<StreamShardHandle> shardsOfStream = new ArrayList<>(shardCount);
					for (int i = 0; i < shardCount; i++) {
						shardsOfStream.add(
							new StreamShardHandle(
								streamName,
								Shard.builder().shardId(KinesisShardIdGenerator.generateFromShardOrder(i)).build()));
					}
					streamsWithListOfShards.put(streamName, shardsOfStream);
				}
			}
		}

		@Override
		public GetShardListResult getShardList(Map<String, String> streamNamesWithLastSeenShardIds) {
			GetShardListResult result = new GetShardListResult();
			for (Map.Entry<String, List<StreamShardHandle>> streamsWithShards : streamsWithListOfShards.entrySet()) {
				String streamName = streamsWithShards.getKey();
				for (StreamShardHandle shard : streamsWithShards.getValue()) {
					if (streamNamesWithLastSeenShardIds.get(streamName) == null) {
						result.addRetrievedShardToStream(streamName, shard);
					} else {
						if (compareShardIds(
							shard.getShard().shardId(), streamNamesWithLastSeenShardIds.get(streamName)) > 0) {
							result.addRetrievedShardToStream(streamName, shard);
						}
					}
				}
			}
			return result;
		}

		@Override
		public String getShardIterator(StreamShardHandle shard, String shardIteratorType, Object startingMarker) {
			return null;
		}

		@Override
		public GetRecordsResponse getRecords(String shardIterator, int maxRecordsToGet) {
			return null;
		}

		/**
		 * Utility function to compare two shard ids.
		 *
		 * @param firstShardId first shard id to compare
		 * @param secondShardId second shard id to compare
		 * @return a value less than 0 if the first shard id is smaller than the second shard id,
		 *         or a value larger than 0 the first shard is larger than the second shard id,
		 *         or 0 if they are equal
		 */
		private static int compareShardIds(String firstShardId, String secondShardId) {
			if (!isValidShardId(firstShardId)) {
				throw new IllegalArgumentException("The first shard id has invalid format.");
			}

			if (!isValidShardId(secondShardId)) {
				throw new IllegalArgumentException("The second shard id has invalid format.");
			}

			// digit segment of the shard id starts at index 8
			return Long.compare(Long.parseLong(firstShardId.substring(8)), Long.parseLong(secondShardId.substring(8)));
		}

		/**
		 * Checks if a shard id has valid format.
		 * Kinesis stream shard ids have 12-digit numbers left-padded with 0's,
		 * prefixed with "shardId-", ex. "shardId-000000000015".
		 *
		 * @param shardId the shard id to check
		 * @return whether the shard id is valid
		 */
		private static boolean isValidShardId(String shardId) {
			if (shardId == null) {
				return false;
			}
			return shardId.matches("^shardId-\\d{12}");
		}
	}

	private static class BlockingQueueKinesis implements KinesisProxyInterface {

		private Map<String, List<StreamShardHandle>> streamsWithListOfShards = new HashMap<>();
		private Map<String, BlockingQueue<String>> shardIteratorToQueueMap = new HashMap<>();

		private static String getShardIterator(StreamShardHandle shardHandle) {
			return shardHandle.getStreamName() + "-" + shardHandle.getShard().shardId();
		}

		public BlockingQueueKinesis(Map<String, List<BlockingQueue<String>>> streamsToShardCount) {
			for (Map.Entry<String, List<BlockingQueue<String>>> streamToShardQueues : streamsToShardCount.entrySet()) {
				String streamName = streamToShardQueues.getKey();
				int shardCount = streamToShardQueues.getValue().size();

				if (shardCount == 0) {
					// don't do anything
				} else {
					List<StreamShardHandle> shardsOfStream = new ArrayList<>(shardCount);
					for (int i = 0; i < shardCount; i++) {
						StreamShardHandle shardHandle = new StreamShardHandle(
							streamName,
							Shard.builder()
								.shardId(KinesisShardIdGenerator.generateFromShardOrder(i))
								.sequenceNumberRange(SequenceNumberRange.builder().startingSequenceNumber("0").build())
								.hashKeyRange(HashKeyRange.builder().startingHashKey("0").endingHashKey("0").build())
								.build());
						shardsOfStream.add(shardHandle);
						shardIteratorToQueueMap.put(getShardIterator(shardHandle), streamToShardQueues.getValue().get(i));
					}
					streamsWithListOfShards.put(streamName, shardsOfStream);
				}
			}
		}

		@Override
		public GetShardListResult getShardList(Map<String, String> streamNamesWithLastSeenShardIds) {
			GetShardListResult result = new GetShardListResult();
			for (Map.Entry<String, List<StreamShardHandle>> streamsWithShards : streamsWithListOfShards.entrySet()) {
				String streamName = streamsWithShards.getKey();
				for (StreamShardHandle shard : streamsWithShards.getValue()) {
					if (streamNamesWithLastSeenShardIds.get(streamName) == null) {
						result.addRetrievedShardToStream(streamName, shard);
					} else {
						if (StreamShardHandle.compareShardIds(
							shard.getShard().shardId(), streamNamesWithLastSeenShardIds.get(streamName)) > 0) {
							result.addRetrievedShardToStream(streamName, shard);
						}
					}
				}
			}
			return result;
		}

		@Override
		public String getShardIterator(StreamShardHandle shard, String shardIteratorType, Object startingMarker) {
			return getShardIterator(shard);
		}

		@Override
		public GetRecordsResponse getRecords(String shardIterator, int maxRecordsToGet) {
			BlockingQueue<String> queue = Preconditions.checkNotNull(this.shardIteratorToQueueMap.get(shardIterator),
			"no queue for iterator %s", shardIterator);
			List<Record> records = Collections.emptyList();
			try {
				String data = queue.take();
				Record record = Record.builder()
					.data(SdkBytes.fromString(data, ConfigConstants.DEFAULT_CHARSET))
					.partitionKey(UUID.randomUUID().toString())
					.approximateArrivalTimestamp(Instant.now())
					.sequenceNumber(String.valueOf(0))
					.build();
				records = Collections.singletonList(record);
			} catch (InterruptedException e) {
				shardIterator = null;
			}
			return GetRecordsResponse.builder()
				.records(records)
				.millisBehindLatest(0L)
				.nextShardIterator(shardIterator)
				.build();
		}
	}

}
