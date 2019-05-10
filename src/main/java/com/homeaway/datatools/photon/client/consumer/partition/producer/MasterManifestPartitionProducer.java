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
import com.homeaway.datatools.photon.dao.beam.BeamDataManifestDao;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;
import com.homeaway.datatools.photon.dao.rows.PhotonRowSetFuture;
import com.homeaway.datatools.photon.utils.client.PartitionProducerType;
import com.homeaway.datatools.photon.utils.consumer.BatchResult;
import com.homeaway.datatools.photon.utils.consumer.ConsumerQueue;
import com.homeaway.datatools.photon.utils.dao.PartitionHelper;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class MasterManifestPartitionProducer extends AbstractPartitionProducer<PartitionManifestResult> {

    private final BeamCache beamCache;
    private final BeamDataManifestDao beamDataManifestDao;
    private final PartitionHelper partitionHelper;
    private final Instant start;
    private final Instant end;

    public MasterManifestPartitionProducer(final BeamCache beamCache,
                                           final BeamDataManifestDao beamDataManifestDao,
                                           final PartitionHelper partitionHelper,
                                           final PartitionProducerType partitionProducerType,
                                           final Instant start,
                                           final Instant end) {
        super(partitionProducerType);
        this.beamCache = beamCache;
        this.beamDataManifestDao = beamDataManifestDao;
        this.partitionHelper = partitionHelper;
        this.start = start;
        this.end = end;
    }

    @Override
    protected void doProducePartitionResults(UUID key,
                                             ConcurrentMap<UUID, ConsumerQueue<PartitionManifestResult>> consumerQueueMap,
                                             PhotonBeamReader photonBeamReader,
                                             ConcurrentMap<UUID, Boolean> producerRunnableFlags) {
        Instant minManifest = partitionHelper.getManifestKey(partitionProducerType.equals(PartitionProducerType.FORWARD) ? start : end);
        Instant maxManifest = partitionHelper.getManifestKey(partitionProducerType.equals(PartitionProducerType.FORWARD) ? end : start);
        Instant masterStart = partitionHelper.getMasterManifestKey(start);
        Instant masterEnd = partitionHelper.getMasterManifestKey(end);

        do {
            addResultToQueue(key, consumerQueueMap.get(key),
                    new BatchResult<>(new PartitionManifestResult(getPartitionDate(masterStart, maxManifest),
                            getFutures(photonBeamReader, masterStart, minManifest, maxManifest, beamCache, beamDataManifestDao)),
                            getIsLastBatch(partitionProducerType, masterStart, masterEnd)),
                    producerRunnableFlags);

            masterStart = partitionProducerType.getManifestPartitionKeyMover().apply(masterStart);
        } while (producerRunnableFlags.get(key)
                && partitionProducerType.getManifestPartitionKeyComparator().apply(masterStart, masterEnd));

        if (!producerRunnableFlags.get(key)) {
            clearQueueAndPoison(key, consumerQueueMap);
        }
    }

    private static Instant getPartitionDate(Instant masterStart, Instant maxManifest) {
        return masterStart.compareTo(maxManifest) > 0 ? maxManifest : masterStart;
    }

    private static boolean getIsLastBatch(PartitionProducerType partitionProducerType, Instant masterStart, Instant masterEnd) {
        return !partitionProducerType.getManifestPartitionKeyComparator().apply(partitionProducerType.getManifestPartitionKeyMover().apply(masterStart), masterEnd);
    }

    private static Map<UUID, PhotonRowSetFuture> getFutures(PhotonBeamReader beamReader,
                                                            Instant masterTime,
                                                            Instant manifestStart,
                                                            Instant end,
                                                            BeamCache beamCache,
                                                            BeamDataManifestDao beamDataManifestDao) {
        Map<UUID, PhotonRowSetFuture> futures = Maps.newLinkedHashMap();
        beamCache.getBeamByName(beamCache.getBeamByUuid(beamReader.getBeamUuid()).get().getBeamName())
                .forEach(b -> futures.put(b.getBeamUuid(), beamDataManifestDao.getBeamMasterManifest(b.getBeamUuid(), masterTime, manifestStart, end)));
        return futures;
    }
}
