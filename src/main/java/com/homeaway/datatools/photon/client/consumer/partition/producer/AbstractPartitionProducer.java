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
package com.homeaway.datatools.photon.client.consumer.partition.producer;

import com.google.common.collect.Maps;
import com.homeaway.datatools.photon.client.consumer.iterator.ManifestEntry;
import com.homeaway.datatools.photon.client.consumer.iterator.ManifestEntryIterator;
import com.homeaway.datatools.photon.client.consumer.iterator.ManifestType;
import com.homeaway.datatools.photon.client.consumer.iterator.MergeIterator;
import com.homeaway.datatools.photon.client.consumer.iterator.PhotonRowSetIterator;
import com.homeaway.datatools.photon.client.consumer.iterator.StatefulIterator;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;
import com.homeaway.datatools.photon.dao.rows.PhotonRow;
import com.homeaway.datatools.photon.dao.rows.PhotonRowSetFuture;
import static com.homeaway.datatools.photon.utils.client.ConsumerUtils.GET_PHOTON_ROWSET_FROM_FUTURE;
import com.homeaway.datatools.photon.utils.client.PartitionProducerType;
import com.homeaway.datatools.photon.utils.consumer.BatchResult;
import com.homeaway.datatools.photon.utils.consumer.ConsumerQueue;
import com.homeaway.datatools.photon.utils.consumer.PartitionProducer;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public abstract class AbstractPartitionProducer<T> implements PartitionProducer<T, PhotonBeamReader> {

    protected final PartitionProducerType partitionProducerType;

    public AbstractPartitionProducer(final PartitionProducerType partitionProducerType) {
        this.partitionProducerType = partitionProducerType;
    }

    @Override
    public void producePartitionResults(UUID key,
                                        ConcurrentMap<UUID, ConsumerQueue<T>> consumerQueueMap,
                                        PhotonBeamReader photonBeamReader,
                                        ConcurrentMap<UUID, Boolean> producerRunnableFlags) {
        try {
            doProducePartitionResults(key, consumerQueueMap, photonBeamReader, producerRunnableFlags);
        } catch (Exception e) {
            log.error("Could not produce partition for {}.", key, e);
            clearQueueAndPoison(key, consumerQueueMap);
        }
    }

    protected abstract void doProducePartitionResults(UUID key,
                                            ConcurrentMap<UUID, ConsumerQueue<T>> consumerQueueMap,
                                            PhotonBeamReader photonBeamReader,
                                            ConcurrentMap<UUID, Boolean> producerRunnableFlags);

    protected void addResultToQueue(UUID key,
                                ConsumerQueue<T> consumerQueue,
                                BatchResult<T> batchResult,
                                ConcurrentMap<UUID, Boolean> producerRunnableFlags) {
        boolean success;
        do {
            try {
                success = consumerQueue.getBatchResultQueue().offer(batchResult, 5, MILLISECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        } while (!success && producerRunnableFlags.get(key));
    }

    void clearQueueAndPoison(UUID key, ConcurrentMap<UUID, ConsumerQueue<T>> consumerQueueMap) {
        consumerQueueMap.get(key).getBatchResultQueue().clear();
        consumerQueueMap.get(key).getBatchResultQueue().offer(new BatchResult<>(null, true));
    }

    Map<UUID, StatefulIterator<PhotonRow>> getResultSetIteratorsFromFutures(final Map<UUID, PhotonRowSetFuture> futures) {
        Map<UUID, StatefulIterator<PhotonRow>> iteratorMap = Maps.newHashMap();
        for(Map.Entry<UUID, PhotonRowSetFuture> e : futures.entrySet()) {
            iteratorMap.put(e.getKey(), new PhotonRowSetIterator(GET_PHOTON_ROWSET_FROM_FUTURE.apply(e.getValue())));
        }
        return iteratorMap;
    }

    MergeIterator<ManifestEntry> getMergeIterator(PartitionManifestResult partitionManifestResult, ManifestType manifestType) {
        return Optional.ofNullable(partitionManifestResult.getPartitions())
                .map(p -> new ManifestEntryIterator(manifestType, getResultSetIteratorsFromFutures(p)))
                .orElse(new ManifestEntryIterator(manifestType, Maps.newHashMap()));
    }
}
