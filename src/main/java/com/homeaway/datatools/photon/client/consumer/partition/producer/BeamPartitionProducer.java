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

import com.google.common.collect.Lists;
import com.homeaway.datatools.photon.client.cache.BeamCache;
import com.homeaway.datatools.photon.client.cache.ProcessedRecordCache;
import com.homeaway.datatools.photon.client.cache.ProcessedRecordCacheEntry;
import com.homeaway.datatools.photon.client.consumer.iterator.ManifestEntry;
import static com.homeaway.datatools.photon.client.consumer.iterator.ManifestType.MANIFEST;
import com.homeaway.datatools.photon.client.consumer.iterator.MergeIterator;
import com.homeaway.datatools.photon.dao.rows.PhotonRowSetFuture;
import com.homeaway.datatools.photon.dao.beam.BeamDataDao;
import com.homeaway.datatools.photon.dao.beam.BeamDataManifestDao;
import com.homeaway.datatools.photon.dao.beam.BeamProcessedDao;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;
import com.homeaway.datatools.photon.utils.client.PartitionProducerType;
import com.homeaway.datatools.photon.utils.client.ProcessedRecordCacheCheck;
import com.homeaway.datatools.photon.utils.client.QueuedMessagesResult;
import com.homeaway.datatools.photon.utils.consumer.BatchResult;
import com.homeaway.datatools.photon.utils.consumer.ConsumerQueue;
import com.homeaway.datatools.photon.utils.consumer.PartitionConsumer;
import com.homeaway.datatools.photon.utils.consumer.PartitionProducer;
import com.homeaway.datatools.photon.utils.dao.PartitionHelper;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class BeamPartitionProducer extends AbstractPartitionProducer<QueuedMessagesResult> {

    private final BeamCache beamCache;
    private final PartitionConsumer<PartitionManifestResult, PhotonBeamReader> masterManifestConsumer;
    private final PartitionConsumer<PartitionManifestResult, PhotonBeamReader> manifestPartitionConsumer;
    private final PartitionHelper partitionHelper;
    private final BeamDataManifestDao beamDataManifestDao;
    private final BeamDataDao beamDataDao;
    private final BeamProcessedDao beamProcessedDao;
    private final ProcessedRecordCache processedRecordCache;
    private final Instant start;
    private final Instant end;


    public BeamPartitionProducer(final BeamCache beamCache,
                                 final PartitionConsumer<PartitionManifestResult, PhotonBeamReader> masterManifestConsumer,
                                 final PartitionConsumer<PartitionManifestResult, PhotonBeamReader> manifestPartitionConsumer,
                                 final PartitionHelper partitionHelper,
                                 final BeamDataManifestDao beamDataManifestDao,
                                 final BeamDataDao beamDataDao,
                                 final BeamProcessedDao beamProcessedDao,
                                 final ProcessedRecordCache processedRecordCache,
                                 final PartitionProducerType partitionProducerType,
                                 final Instant start,
                                 final Instant end) {
        super(partitionProducerType);
        this.beamCache = beamCache;
        this.masterManifestConsumer = masterManifestConsumer;
        this.manifestPartitionConsumer = manifestPartitionConsumer;
        this.partitionHelper = partitionHelper;
        this.beamDataManifestDao = beamDataManifestDao;
        this.beamDataDao = beamDataDao;
        this.beamProcessedDao = beamProcessedDao;
        this.processedRecordCache = processedRecordCache;
        this.start = start;
        this.end = end;
    }

    protected void doProducePartitionResults(UUID key,
                                        ConcurrentMap<UUID, ConsumerQueue<QueuedMessagesResult>> consumerQueueMap,
                                        PhotonBeamReader photonBeamReader,
                                        ConcurrentMap<UUID, Boolean> producerRunnableFlags) {
        PartitionProducer<PartitionManifestResult, PhotonBeamReader> manifestPartitionProducer =
                new ManifestPartitionProducer(beamCache, beamDataManifestDao, partitionHelper, partitionProducerType, masterManifestConsumer, start, end);

        manifestPartitionConsumer.init(key,
                photonBeamReader,
                manifestPartitionProducer);

        try {
            BatchResult<PartitionManifestResult> batchResult;
            do {
                batchResult = manifestPartitionConsumer.fetchBatch(key);

                processPartitions(key, photonBeamReader, consumerQueueMap.get(key),
                        batchResult, producerRunnableFlags);
            } while (producerRunnableFlags.get(key)
                    && (!batchResult.isLastBatch()));

            if (!producerRunnableFlags.get(key)) {
                clearQueueAndPoison(key, consumerQueueMap);
            }
        } catch (Exception e) {
            masterManifestConsumer.shutDownProducer(key);
            manifestPartitionConsumer.shutDownProducer(key);
            producerRunnableFlags.put(key, Boolean.FALSE);
            throw new RuntimeException(e);
        }
    }

    private void processPartitions(UUID key,
                                   PhotonBeamReader photonBeamReader,
                                   ConsumerQueue<QueuedMessagesResult> consumerQueue,
                                   BatchResult<PartitionManifestResult> batchResult,
                                   ConcurrentMap<UUID, Boolean> producerRunnableFlags) {

        Optional<PartitionManifestResult> partitionManifestResult = batchResult.getResults();
        if (partitionManifestResult.isPresent()) {
            MergeIterator<ManifestEntry> manifestEntryIterator = getMergeIterator(partitionManifestResult.get(), MANIFEST);
            Optional<ManifestEntry> entry = manifestEntryIterator.getNextEntry();
            if (entry.isPresent()) {
                while (entry.isPresent()) {
                    addResultToQueue(key, consumerQueue,
                            new BatchResult<>(buildMessageResult(entry.get().getBeamUuids(), photonBeamReader, entry.get().getPartitionTime()),
                                    batchResult.isLastBatch() && manifestEntryIterator.isExhausted()),
                            producerRunnableFlags);

                    entry = manifestEntryIterator.getNextEntry();
                }
            } else {
                addResultToQueue(key, consumerQueue, new BatchResult<>(new QueuedMessagesResult(partitionManifestResult.get().getPartitionTime(), null, null), batchResult.isLastBatch()),
                        producerRunnableFlags);
            }
        } else {
            addResultToQueue(key, consumerQueue, new BatchResult<>(null, batchResult.isLastBatch()), producerRunnableFlags);
        }
    }

    private QueuedMessagesResult buildMessageResult(List<UUID> beamUuids, PhotonBeamReader photonBeamReader, Instant partition) {
        ProcessedRecordCacheCheck check = new ProcessedRecordCacheCheck();
        Optional<ProcessedRecordCacheEntry> processedRecordCacheEntry = processedRecordCache.getProcessedEntities(photonBeamReader, partition);
        if (processedRecordCacheEntry.isPresent()) {
            check.setProcessedRecordCacheEntry(processedRecordCacheEntry.get());
        } else {
            check.setProcessedRecords(beamProcessedDao.getProcessedMessages(photonBeamReader.getBeamReaderUuid(), partition));
        }
        return new QueuedMessagesResult(partition, getQueueFutures(beamUuids, partition), check);
    }

    private List<PhotonRowSetFuture> getQueueFutures(List<UUID> beamUuids, Instant partition) {
        List<PhotonRowSetFuture> photonRowSetFutures = Lists.newArrayList();
        for(UUID b : beamUuids) {
            photonRowSetFutures.add(beamDataDao.getQueuePartitionAsync(b, partition));
        }
        return photonRowSetFutures;
    }

}
