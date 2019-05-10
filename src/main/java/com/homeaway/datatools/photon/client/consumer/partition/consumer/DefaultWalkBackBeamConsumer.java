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
package com.homeaway.datatools.photon.client.consumer.partition.consumer;

import com.google.common.collect.Maps;
import com.homeaway.datatools.photon.api.beam.PhotonMessageHandler;
import com.homeaway.datatools.photon.api.beam.BeamException;
import com.homeaway.datatools.photon.api.beam.PhotonMessage;
import com.homeaway.datatools.photon.api.model.PhotonBeamReaderLockWrapper;
import com.homeaway.datatools.photon.client.cache.BeamCache;
import com.homeaway.datatools.photon.client.cache.BeamReaderCache;
import com.homeaway.datatools.photon.client.cache.DefaultProcessedRecordCache;
import com.homeaway.datatools.photon.client.cache.ProcessedRecordCache;
import com.homeaway.datatools.photon.client.cache.ProcessedRecordCacheEntry;
import com.homeaway.datatools.photon.client.consumer.PhotonBeamReaderConfig;
import com.homeaway.datatools.photon.client.consumer.iterator.MergeIterator;
import com.homeaway.datatools.photon.client.consumer.iterator.PartitionIterator;
import com.homeaway.datatools.photon.client.consumer.partition.producer.BeamPartitionProducer;
import com.homeaway.datatools.photon.client.consumer.partition.producer.PartitionManifestResult;
import com.homeaway.datatools.photon.dao.beam.BeamDataManifestDao;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import com.homeaway.datatools.photon.dao.rows.PhotonRow;
import com.homeaway.datatools.photon.dao.beam.BeamProcessedDao;
import com.homeaway.datatools.photon.dao.beam.BeamDataDao;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;
import com.homeaway.datatools.photon.serialization.PhotonDeserializer;
import static com.homeaway.datatools.photon.utils.client.ClientConstants.INTERVAL_SECONDS;
import static com.homeaway.datatools.photon.utils.client.PartitionProducerType.REVERSE;
import com.homeaway.datatools.photon.utils.client.PhotonBeamWalkBackTracker;
import com.homeaway.datatools.photon.utils.client.QueuedMessagesResult;
import com.homeaway.datatools.photon.utils.consumer.BasePartitionConsumer;
import com.homeaway.datatools.photon.utils.consumer.BatchResult;
import com.homeaway.datatools.photon.utils.consumer.PartitionConsumer;
import com.homeaway.datatools.photon.utils.consumer.PartitionProducer;
import com.homeaway.datatools.photon.utils.dao.PartitionHelper;
import static java.lang.Boolean.TRUE;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DefaultWalkBackBeamConsumer extends AbstractBeamConsumer implements WalkBackBeamConsumer {

    private final BeamReaderCache beamReaderCache;
    private final ConcurrentMap<PhotonBeamReaderConfig, PhotonBeamWalkBackTracker> walkBackTrackers;
    private final ConcurrentMap<UUID, Instant> nextWalkBackEnd;
    private final ScheduledExecutorService scheduledExecutorService;
    private final ExecutorService executorService;
    private volatile boolean active;

    public DefaultWalkBackBeamConsumer(final PhotonDeserializer photonDeserializer,
                                       final BeamCache beamCache,
                                       final BeamReaderCache beamReaderCache,
                                       final BeamDataDao beamDataDao,
                                       final BeamDataManifestDao beamDataManifestDao,
                                       final BeamProcessedDao beamProcessedDao,
                                       final PartitionHelper partitionHelper) {
        this(photonDeserializer, beamCache, beamReaderCache, beamDataDao, beamDataManifestDao, beamProcessedDao,
                new BasePartitionConsumer<>(5),
                new BasePartitionConsumer<>(10),
                new BasePartitionConsumer<>(100),
                partitionHelper);
    }

    public DefaultWalkBackBeamConsumer(final PhotonDeserializer photonDeserializer,
                                       final BeamCache beamCache,
                                       final BeamReaderCache beamReaderCache,
                                       final BeamDataDao beamDataDao,
                                       final BeamDataManifestDao beamDataManifestDao,
                                       final BeamProcessedDao beamProcessedDao,
                                       final PartitionConsumer<PartitionManifestResult, PhotonBeamReader> masterManifestConsumer,
                                       final PartitionConsumer<PartitionManifestResult, PhotonBeamReader> partitionManifestConsumer,
                                       final PartitionConsumer<QueuedMessagesResult, PhotonBeamReader> partitionConsumer,
                                       final PartitionHelper partitionHelper) {
        this(photonDeserializer, beamCache, beamReaderCache, beamDataDao, beamDataManifestDao, beamProcessedDao, masterManifestConsumer, partitionManifestConsumer, partitionConsumer,
                new DefaultProcessedRecordCache(beamProcessedDao), partitionHelper);
    }

    public DefaultWalkBackBeamConsumer(final PhotonDeserializer photonDeserializer,
                                       final BeamCache beamCache,
                                       final BeamReaderCache beamReaderCache,
                                       final BeamDataDao beamDataDao,
                                       final BeamDataManifestDao beamDataManifestDao,
                                       final BeamProcessedDao beamProcessedDao,
                                       final PartitionConsumer<PartitionManifestResult, PhotonBeamReader> masterManifestConsumer,
                                       final PartitionConsumer<PartitionManifestResult, PhotonBeamReader> partitionManifestConsumer,
                                       final PartitionConsumer<QueuedMessagesResult, PhotonBeamReader> partitionConsumer,
                                       final ProcessedRecordCache processedRecordCache,
                                       final PartitionHelper partitionHelper) {
        super(photonDeserializer, beamCache, beamDataDao, beamDataManifestDao, beamProcessedDao, processedRecordCache, partitionHelper, masterManifestConsumer,
                partitionManifestConsumer, partitionConsumer);
        this.beamReaderCache = beamReaderCache;
        this.nextWalkBackEnd = Maps.newConcurrentMap();
        this.walkBackTrackers = Maps.newConcurrentMap();
        this.scheduledExecutorService = Executors.newScheduledThreadPool(50);
        this.executorService = Executors.newFixedThreadPool(50);
    }

    @Override
    public void removeBeamFromWalking(PhotonBeamReaderConfig photonBeamReaderConfig) {
        walkBackTrackers.remove(photonBeamReaderConfig);
    }

    @Override
    public void putBeamForWalking(PhotonBeamReaderConfig photonBeamReaderConfig, Duration walkBackThreshold) {
        if (!walkBackTrackers.containsKey(photonBeamReaderConfig)) {
            PhotonBeamWalkBackTracker photonBeamWalkBackTracker =
                    buildBeamBackoffTracker(photonBeamReaderConfig.getPhotonMessageHandler(), walkBackThreshold);
            walkBackTrackers.put(photonBeamReaderConfig, photonBeamWalkBackTracker);
        }
    }

    @Override
    public void start() {
        if (!active) {
            scheduledExecutorService.execute(new BackoffProcessor());
            active = true;
        }
    }

    @Override
    public void stop() {
        active = false;
    }

    private void consume(PhotonBeamReader photonBeamReader,
                         PhotonBeamWalkBackTracker photonBeamWalkBackTracker) {

        PartitionProducer<QueuedMessagesResult, PhotonBeamReader> partitionProducer = new BeamPartitionProducer(beamCache, masterManifestConsumer,
                partitionManifestConsumer, partitionHelper, beamDataManifestDao, beamDataDao, beamProcessedDao, processedRecordCache, REVERSE,
                photonBeamWalkBackTracker.getStart(), photonBeamWalkBackTracker.getEnd());

        partitionConsumer.init(photonBeamWalkBackTracker.getBackoffKey(), photonBeamReader, partitionProducer);

        while (true) {
            if (!photonBeamWalkBackTracker.isActive()) {
                stopConsumers(photonBeamWalkBackTracker.getBackoffKey());
                return;
            }
            BatchResult<QueuedMessagesResult> batchResult = partitionConsumer.fetchBatch(photonBeamWalkBackTracker.getBackoffKey());

            if (batchResult.getResults().isPresent()) {
                QueuedMessagesResult queuedMessagesResults = batchResult.getResults().get();
                processResults(photonBeamReader, photonBeamWalkBackTracker, queuedMessagesResults);
            }
        }
    }

    private PhotonBeamWalkBackTracker buildBeamBackoffTracker(PhotonMessageHandler photonMessageHandler,
                                                              Duration walkBackThreshold) {
        PhotonBeamWalkBackTracker photonBeamWalkBackTracker = new PhotonBeamWalkBackTracker();
        photonBeamWalkBackTracker.setPhotonMessageHandler(photonMessageHandler);
        photonBeamWalkBackTracker.setBackoffKey(UUID.randomUUID());
        photonBeamWalkBackTracker.setWalkBackThreshold(walkBackThreshold);
        return photonBeamWalkBackTracker;
    }

    private void processResults(PhotonBeamReader photonBeamReader,
                                PhotonBeamWalkBackTracker photonBeamWalkBackTracker,
                                QueuedMessagesResult queuedMessagesResult) {

        try {
            if (queuedMessagesResult.getQueueMessages().isPresent()) {
                MergeIterator<PhotonRow> entryIterator = new PartitionIterator(getStatefulIterators(queuedMessagesResult.getQueueMessages().get()));

                ProcessedRecordCacheEntry processedRecordCacheEntry = queuedMessagesResult.getProcessedRecordCacheCheck().getProcessedRecordCacheEntry()
                        .orElseGet(() -> translateProcessedResultSetToSet(photonBeamReader.getBeamReaderUuid(),
                                queuedMessagesResult.getPartitionTime(),
                                queuedMessagesResult.getProcessedRecordCacheCheck().getProcessedRecords()));

                Optional<PhotonRow> row = entryIterator.getNextEntry();
                if (row.isPresent()) {
                    while (row.isPresent()) {

                        PhotonMessage photonMessage = getPhotonMessage(photonBeamReader, row.get());

                        if (!processedRecordCacheEntry.isPresent(photonMessage.getWriteTime(), photonMessage.getMessageKey())) {
                            photonBeamWalkBackTracker.getPhotonMessageHandler().handleStaleMessage(photonMessage);

                            if (photonMessage.getWriteTime().isAfter(nextWalkBackEnd.computeIfAbsent(photonBeamReader.getBeamReaderUuid(),
                                    k -> photonBeamReader.getWaterMark()))) {
                                nextWalkBackEnd.put(photonBeamReader.getBeamReaderUuid(), photonMessage.getWriteTime());
                            }
                            processedRecordCache.putEntry(photonMessage, TRUE);
                        }
                        row = entryIterator.getNextEntry();
                    }

                }
            }

        } catch (Exception e) {
            String errorMessage = String.format("Could not process partition: beam = [%s], partition = [%s]", photonBeamReader.getBeamUuid(), queuedMessagesResult.getPartitionTime());
            photonBeamWalkBackTracker.getPhotonMessageHandler().handleException(new BeamException(errorMessage, e));
            stopConsumers(photonBeamWalkBackTracker.getBackoffKey());
        }
    }

    private class BackoffProcessor implements Runnable {

        @Override
        public void run() {
            walkBackTrackers
                    .entrySet()
                    .forEach(e -> {
                        Queue<PhotonBeam> beams = beamCache.getBeamByName(e.getKey().getBeamName());
                        if (!beams.isEmpty()) {
                            Optional<PhotonBeamReaderLockWrapper> wrapper = beamReaderCache.getPhotonBeamReader(e.getKey().getClientName(), beams.peek());
                            if (wrapper.isPresent()) {
                                PhotonBeamReader photonBeamReader = wrapper.get().getPhotonBeamReader();
                                if (photonBeamReader.getPhotonBeamReaderLock().isPresent()) {
                                    if (!e.getValue().isActive()) {
                                        if (e.getValue().getStart() == null) {
                                            e.getValue().setStart(photonBeamReader.getWaterMark());
                                            e.getValue().setEnd(photonBeamReader.getWaterMark().minus(e.getValue().getWalkBackThreshold()));
                                        }
                                        e.getValue().setActive(true);
                                        executorService.execute(new BackoffConsumerRunnable(photonBeamReader, e.getValue()));
                                    }
                                }
                            }
                        }
                    });

            if (active) {
                scheduledExecutorService.schedule(this, INTERVAL_SECONDS, TimeUnit.SECONDS);
            }
        }
    }

    private class BackoffConsumerRunnable implements Runnable {

        private final PhotonBeamReader photonBeamReader;
        private final PhotonBeamWalkBackTracker photonBeamWalkBackTracker;

        public BackoffConsumerRunnable(final PhotonBeamReader photonBeamReader,
                                       final PhotonBeamWalkBackTracker photonBeamWalkBackTracker) {
            this.photonBeamReader = photonBeamReader;
            this.photonBeamWalkBackTracker = photonBeamWalkBackTracker;
        }

        @Override
        public void run() {
            try {
                consume(photonBeamReader, photonBeamWalkBackTracker);
            } catch (Exception e) {
                log.error("Error processing backoff for beam reader = {}", photonBeamReader.getBeamReaderUuid(), e);
            } finally {
                if (photonBeamWalkBackTracker.isActive()) {
                    photonBeamWalkBackTracker.setStart(photonBeamReader.getWaterMark());
                    photonBeamWalkBackTracker.setActive(false);
                }
            }
        }
    }
}
