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
import com.homeaway.datatools.photon.client.cache.BeamCache;
import com.homeaway.datatools.photon.client.consumer.iterator.ManifestEntry;
import static com.homeaway.datatools.photon.client.consumer.iterator.ManifestType.MASTER;
import com.homeaway.datatools.photon.client.consumer.iterator.MergeIterator;
import com.homeaway.datatools.photon.dao.beam.BeamDataManifestDao;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;
import com.homeaway.datatools.photon.dao.rows.PhotonRowSetFuture;
import com.homeaway.datatools.photon.utils.client.PartitionProducerType;
import com.homeaway.datatools.photon.utils.consumer.BatchResult;
import com.homeaway.datatools.photon.utils.consumer.ConsumerQueue;
import com.homeaway.datatools.photon.utils.consumer.PartitionConsumer;
import com.homeaway.datatools.photon.utils.consumer.PartitionProducer;
import com.homeaway.datatools.photon.utils.dao.PartitionHelper;
import static java.lang.Boolean.FALSE;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class ManifestPartitionProducer extends AbstractPartitionProducer<PartitionManifestResult> implements PartitionProducer<PartitionManifestResult, PhotonBeamReader> {

    private final BeamCache beamCache;
    private final BeamDataManifestDao beamDataManifestDao;
    private final PartitionHelper partitionHelper;
    private final PartitionConsumer<PartitionManifestResult, PhotonBeamReader> masterManifestConsumer;
    private final Instant start;
    private final Instant end;

    public ManifestPartitionProducer(final BeamCache beamCache,
                                     final BeamDataManifestDao beamDataManifestDao,
                                     final PartitionHelper partitionHelper,
                                     final PartitionProducerType partitionProducerType,
                                     final PartitionConsumer<PartitionManifestResult, PhotonBeamReader> masterManifestConsumer,
                                     final Instant start,
                                     final Instant end) {
        super(partitionProducerType);
        this.beamCache = beamCache;
        this.beamDataManifestDao = beamDataManifestDao;
        this.partitionHelper = partitionHelper;
        this.masterManifestConsumer = masterManifestConsumer;
        this.start = start;
        this.end = end;
    }

    public void doProducePartitionResults(UUID key,
                                        ConcurrentMap<UUID, ConsumerQueue<PartitionManifestResult>> consumerQueueMap,
                                        PhotonBeamReader photonBeamReader,
                                        ConcurrentMap<UUID, Boolean> producerRunnableFlags) {

        PartitionProducer<PartitionManifestResult, PhotonBeamReader> masterManifestProducer =
                new MasterManifestPartitionProducer(beamCache, beamDataManifestDao, partitionHelper, partitionProducerType, start, end);

        masterManifestConsumer.init(key, photonBeamReader, masterManifestProducer);

        try {
            BatchResult<PartitionManifestResult> batchResult;
            do {
                batchResult = masterManifestConsumer.fetchBatch(key);

                Optional<PartitionManifestResult> partitionManifestResult = batchResult.getResults();
                if (partitionManifestResult.isPresent()) {
                    MergeIterator<ManifestEntry> masterManifestIterator = getMergeIterator(partitionManifestResult.get(), MASTER);
                    Optional<ManifestEntry> entry = masterManifestIterator.getNextEntry();
                    if (entry.isPresent()) {
                        while(entry.isPresent()) {
                            addResultToQueue(key, consumerQueueMap.get(key),
                                    new BatchResult<>(new PartitionManifestResult(entry.get().getPartitionTime(),
                                            getFutures(photonBeamReader, entry.get().getPartitionTime(), end, beamCache, beamDataManifestDao)),
                                            batchResult.isLastBatch() && masterManifestIterator.isExhausted()),
                                    producerRunnableFlags);

                            entry = masterManifestIterator.getNextEntry();
                        }
                    } else {
                        addResultToQueue(key, consumerQueueMap.get(key),
                                new BatchResult<>(new PartitionManifestResult(partitionManifestResult.get().getPartitionTime(),
                                        null),
                                        batchResult.isLastBatch()),
                                producerRunnableFlags);
                    }
                } else {
                    addResultToQueue(key, consumerQueueMap.get(key), new BatchResult<>(null, batchResult.isLastBatch()), producerRunnableFlags);
                }
            } while(producerRunnableFlags.get(key)
                    && (!batchResult.isLastBatch()));

            if (!producerRunnableFlags.get(key)) {
                clearQueueAndPoison(key, consumerQueueMap);
            }
        } catch (Exception e) {
            masterManifestConsumer.shutDownProducer(key);
            producerRunnableFlags.put(key, FALSE);
            throw new RuntimeException(e);
        }
    }

    private static Map<UUID, PhotonRowSetFuture> getFutures(PhotonBeamReader beamReader,
                                                            Instant manifestStart,
                                                            Instant end,
                                                            BeamCache beamCache,
                                                            BeamDataManifestDao beamDataManifestDao) {
        Map<UUID, PhotonRowSetFuture> futures = Maps.newLinkedHashMap();
        beamCache.getBeamByName(beamCache.getBeamByUuid(beamReader.getBeamUuid()).get().getBeamName())
                .forEach(b -> futures.put(b.getBeamUuid(), beamDataManifestDao.getBeamDataManifest(b.getBeamUuid(), manifestStart, end)));
        return futures;
    }
}
