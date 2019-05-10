/* Copyright (c) 2019 Expedia Group.
 * All rights reserved.  http://www.homeaway.com

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *      http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.homeaway.datatools.photon.utils.consumer;

import com.google.common.collect.Maps;
import lombok.Data;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

public class BasePartitionConsumerTest {

    private PartitionConsumer<UUID, TestReader> partitionConsumer;
    private ConcurrentMap<UUID, ConsumerQueue<UUID>> consumerQueueMap;
    private ConcurrentMap<UUID, Boolean> producerRunnableFlags;

    @Before
    public void init() {
        consumerQueueMap = Maps.newConcurrentMap();
        producerRunnableFlags = Maps.newConcurrentMap();
        partitionConsumer = new BasePartitionConsumer<>(100, consumerQueueMap, producerRunnableFlags);
    }

    @Test
    public void initTest() {
        TestReader testReader = new TestReader();
        testReader.setReaderUuid(UUID.randomUUID());
        partitionConsumer.init(testReader.getReaderUuid(), testReader,
                (key, consumerQueueMap, reader, producerRunnableFlags) -> consumerQueueMap.get(key).getBatchResultQueue().offer(new BatchResult<>(UUID.randomUUID(), true)));

        Assert.assertEquals(1, consumerQueueMap.size());
    }

    @Test
    public void fetchBatchTest() {
        TestReader testReader = new TestReader();
        testReader.setReaderUuid(UUID.randomUUID());
        partitionConsumer.init(testReader.getReaderUuid(), testReader,
                (key, consumerQueueMap, reader, producerRunnableFlags) ->
                        consumerQueueMap.get(key).getBatchResultQueue().offer(new BatchResult<>(UUID.randomUUID(), true)));


        try {
            Thread.sleep(10);
            BatchResult<UUID> batchResult = partitionConsumer.fetchBatch(testReader.getReaderUuid());
            Assert.assertNotNull(batchResult);
            Assert.assertTrue(batchResult.getResults().isPresent());
            Assert.assertTrue(batchResult.isLastBatch());
        } catch (InterruptedException e) {

        }
    }

    @Test
    public void putProducerRunnableFlagTest() {
        TestReader testReader = new TestReader();
        testReader.setReaderUuid(UUID.randomUUID());
        partitionConsumer.init(testReader.getReaderUuid(), testReader,
                (key, consumerQueueMap, reader, producerRunnableFlags) -> {
                    int i = 0;
                    while (producerRunnableFlags.get(key) &&
                            i < 10) {
                        consumerQueueMap.get(key).getBatchResultQueue().offer(new BatchResult<>(UUID.randomUUID(), false));
                        i++;
                    }

                    if(!producerRunnableFlags.get(key)) {
                        consumerQueueMap.get(key).getBatchResultQueue().clear();
                        consumerQueueMap.get(key).getBatchResultQueue().offer(new BatchResult<>(null, true));
                    }
                });

        partitionConsumer.shutDownProducer(testReader.getReaderUuid());
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        BatchResult<UUID> batchResult = partitionConsumer.fetchBatch(testReader.getReaderUuid());
        Assert.assertNotNull(batchResult);
        Assert.assertFalse(batchResult.getResults().isPresent());
        Assert.assertTrue(batchResult.isLastBatch());
    }

    @Test
    public void partitionProducerFailureTest() {
        TestReader testReader = new TestReader();
        testReader.setReaderUuid(UUID.randomUUID());
        partitionConsumer.init(testReader.getReaderUuid(), testReader,
                (key, consumerQueueMap, reader, producerRunnableFlags) -> {
                    throw new RuntimeException("Test");
                });

        Assert.assertEquals(1, consumerQueueMap.size());
    }

    @Data
    private class TestReader {

        UUID readerUuid;
    }
}
