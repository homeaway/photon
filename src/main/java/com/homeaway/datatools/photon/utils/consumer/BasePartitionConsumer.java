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
import com.google.common.collect.Queues;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class BasePartitionConsumer<T, K> implements PartitionConsumer<T, K> {

    private final ExecutorService executorService;
    private final ConcurrentMap<UUID, ConsumerQueue<T>> consumerQueueMap;
    private final ConcurrentMap<UUID, Boolean> producerRunnableFlags;
    private final Integer maxQueueSize;

    public BasePartitionConsumer(final Integer maxQueueSize) {
        this(maxQueueSize, Maps.newConcurrentMap(), Maps.newConcurrentMap());
    }

    BasePartitionConsumer(final Integer maxQueueSize,
                          final ConcurrentMap<UUID, ConsumerQueue<T>> consumerQueueMap,
                          final ConcurrentMap<UUID, Boolean> producerRunnableFlags) {
        this(maxQueueSize, consumerQueueMap, producerRunnableFlags, Executors.newFixedThreadPool(50));
    }

    private BasePartitionConsumer(final Integer maxQueueSize,
                                  final ConcurrentMap<UUID, ConsumerQueue<T>> consumerQueueMap,
                                  final ConcurrentMap<UUID, Boolean> producerRunnableFlags,
                                  final ExecutorService executorService) {
        this.maxQueueSize = maxQueueSize;
        this.consumerQueueMap = consumerQueueMap;
        this.producerRunnableFlags = producerRunnableFlags;
        this.executorService = executorService;
    }

    @Override
    public void shutDownProducer(UUID key) {
        producerRunnableFlags.put(key, false);
    }

    @Override
    public void init(UUID key,
                     K reader,
                     PartitionProducer<T, K> partitionProducer) {
        consumerQueueMap.put(key, new ConsumerQueue<>(Queues.newArrayBlockingQueue(maxQueueSize)));
        producerRunnableFlags.put(key, true);
        executorService.execute(new ProducerRunnable(reader, key, consumerQueueMap, partitionProducer));
    }

    @Override
    public BatchResult<T> fetchBatch(UUID key) {
        try {
            return consumerQueueMap.get(key).getBatchResultQueue().take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private class ProducerRunnable implements Runnable {

        private final K reader;
        private final UUID key;
        private final ConcurrentMap<UUID, ConsumerQueue<T>> consumerQueueMap;
        private final PartitionProducer<T, K> partitionProducer;

        public ProducerRunnable(final K reader,
                                final UUID key,
                                final ConcurrentMap<UUID, ConsumerQueue<T>> consumerQueueMap,
                                final PartitionProducer<T, K> partitionProducer) {
            this.reader = reader;
            this.key = key;
            this.consumerQueueMap = consumerQueueMap;
            this.partitionProducer = partitionProducer;
        }

        @Override
        public void run() {
            try {
                partitionProducer.producePartitionResults(key, consumerQueueMap, reader, producerRunnableFlags);
            } catch (Exception e) {
                log.error("Could not produce batch", e);
                clearBatchResultQueue(consumerQueueMap.get(key));
            }
        }

        private void clearBatchResultQueue(ConsumerQueue<T> consumerQueue) {
            producerRunnableFlags.put(key, false);
            consumerQueue.getBatchResultQueue().clear();
            consumerQueue.getBatchResultQueue().offer(new BatchResult<>(null, true));
        }
    }
}
