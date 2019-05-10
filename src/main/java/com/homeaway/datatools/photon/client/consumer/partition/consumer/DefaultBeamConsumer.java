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

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.UUID;

import com.homeaway.datatools.photon.api.beam.BeamException;
import com.homeaway.datatools.photon.api.beam.PhotonMessage;
import com.homeaway.datatools.photon.api.beam.PhotonMessageHandler;
import com.homeaway.datatools.photon.client.cache.BeamCache;
import com.homeaway.datatools.photon.client.cache.DefaultProcessedRecordCache;
import com.homeaway.datatools.photon.client.cache.ProcessedRecordCache;
import com.homeaway.datatools.photon.client.cache.ProcessedRecordCacheEntry;
import com.homeaway.datatools.photon.client.consumer.iterator.MergeIterator;
import com.homeaway.datatools.photon.client.consumer.iterator.PartitionIterator;
import com.homeaway.datatools.photon.client.consumer.partition.consumer.watermark.BaseOffsetManager;
import com.homeaway.datatools.photon.client.consumer.partition.consumer.watermark.BaseWatermarkManager;
import com.homeaway.datatools.photon.client.consumer.partition.consumer.watermark.DefaultWatermarkUpdater;
import com.homeaway.datatools.photon.client.consumer.partition.consumer.watermark.OffsetManager;
import com.homeaway.datatools.photon.client.consumer.partition.consumer.watermark.WatermarkManager;
import com.homeaway.datatools.photon.client.consumer.partition.consumer.watermark.WatermarkUpdater;
import com.homeaway.datatools.photon.client.consumer.partition.producer.BeamPartitionProducer;
import com.homeaway.datatools.photon.client.consumer.partition.producer.PartitionManifestResult;
import com.homeaway.datatools.photon.dao.rows.PhotonRow;
import com.homeaway.datatools.photon.dao.beam.BeamDataManifestDao;
import com.homeaway.datatools.photon.dao.beam.BeamProcessedDao;
import com.homeaway.datatools.photon.dao.beam.BeamDataDao;
import com.homeaway.datatools.photon.dao.beam.BeamReaderDao;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;
import com.homeaway.datatools.photon.serialization.PhotonDeserializer;
import static com.homeaway.datatools.photon.utils.client.ClientConstants.WATERMAKR_MANAGER_WINDOW_UNIT;
import static com.homeaway.datatools.photon.utils.client.ClientConstants.WATERMARK_MANAGER_WINDOW;
import static com.homeaway.datatools.photon.utils.client.PartitionProducerType.FORWARD;
import com.homeaway.datatools.photon.utils.client.QueuedMessagesResult;
import com.homeaway.datatools.photon.utils.consumer.BasePartitionConsumer;
import com.homeaway.datatools.photon.utils.consumer.BatchResult;
import com.homeaway.datatools.photon.utils.consumer.PartitionConsumer;
import com.homeaway.datatools.photon.utils.consumer.PartitionProducer;
import com.homeaway.datatools.photon.utils.dao.PartitionHelper;
import static java.lang.Boolean.FALSE;
import static java.lang.Long.max;
import lombok.extern.slf4j.Slf4j;

/**
 * The DefaultBeamConsumer is an implementation of the DefaultBeamConsumer interface that is intended to read messages from
 * an Apache Cassandra implementation of a beam. The reader implements a non-blocking asynchronous com.homeaway.datatools.photon.client.consumer.DefaultBeamConsumer
 * that accepts and implementation of the PhotonMessageHandler interface and then makes calls back to that handler implementation as
 * events are read from the queue.
 */
@Slf4j
public class DefaultBeamConsumer extends AbstractBeamConsumer implements BeamConsumer {

    private final WatermarkManager watermarkManager;
    private final WatermarkUpdater watermarkUpdater;
    private final OffsetManager offsetManager;

    public DefaultBeamConsumer(final PhotonDeserializer photonDeserializer,
                               final BeamCache beamCache,
                               final BeamDataDao beamDataDao,
                               final BeamDataManifestDao beamDataManifestDao,
                               final BeamProcessedDao beamProcessedDao,
                               final BeamReaderDao beamReaderDao,
                               final PartitionHelper partitionHelper) {
        this(photonDeserializer, beamCache, beamDataDao, beamDataManifestDao, beamProcessedDao, beamReaderDao,
                new DefaultProcessedRecordCache(beamProcessedDao),
                new BasePartitionConsumer<>(5),
                new BasePartitionConsumer<>(10),
                new BasePartitionConsumer<>(100),
                new BaseWatermarkManager(WATERMARK_MANAGER_WINDOW, WATERMAKR_MANAGER_WINDOW_UNIT),
                partitionHelper);
    }

    public DefaultBeamConsumer(final PhotonDeserializer photonDeserializer,
                               final BeamCache beamCache,
                               final BeamDataDao beamDataDao,
                               final BeamDataManifestDao beamDataManifestDao,
                               final BeamProcessedDao beamProcessedDao,
                               final BeamReaderDao beamReaderDao,
                               final ProcessedRecordCache processedRecordCache,
                               final PartitionHelper partitionHelper) {
        this(photonDeserializer, beamCache, beamDataDao, beamDataManifestDao, beamProcessedDao, beamReaderDao, processedRecordCache,
                new BasePartitionConsumer<>(5),
                new BasePartitionConsumer<>(10),
                new BasePartitionConsumer<>(100),
                new BaseWatermarkManager(WATERMARK_MANAGER_WINDOW, WATERMAKR_MANAGER_WINDOW_UNIT),
                partitionHelper);
    }

    public DefaultBeamConsumer(final PhotonDeserializer photonDeserializer,
                               final BeamCache beamCache,
                               final BeamDataDao beamDataDao,
                               final BeamDataManifestDao beamDataManifestDao,
                               final BeamProcessedDao beamProcessedDao,
                               final BeamReaderDao beamReaderDao,
                               final ProcessedRecordCache processedRecordCache,
                               final PartitionConsumer<PartitionManifestResult, PhotonBeamReader> masterManifestConsumer,
                               final PartitionConsumer<PartitionManifestResult, PhotonBeamReader> partitionManifestConsumer,
                               final PartitionConsumer<QueuedMessagesResult, PhotonBeamReader> partitionConsumer,
                               final WatermarkManager watermarkManager,
                               final PartitionHelper partitionHelper) {
        this(photonDeserializer, beamCache, beamDataDao, beamDataManifestDao, beamProcessedDao, masterManifestConsumer, partitionManifestConsumer, partitionConsumer, watermarkManager, processedRecordCache,
                new DefaultWatermarkUpdater(beamReaderDao), new BaseOffsetManager(beamReaderDao), partitionHelper);
    }

    public DefaultBeamConsumer(final PhotonDeserializer photonDeserializer,
                               final BeamCache beamCache,
                               final BeamDataDao beamDataDao,
                               final BeamDataManifestDao beamDataManifestDao,
                               final BeamProcessedDao beamProcessedDao,
                               final PartitionConsumer<PartitionManifestResult, PhotonBeamReader> masterManifestConsumer,
                               final PartitionConsumer<PartitionManifestResult, PhotonBeamReader> partitionManifestConsumer,
                               final PartitionConsumer<QueuedMessagesResult, PhotonBeamReader> partitionConsumer,
                               final WatermarkManager watermarkManager,
                               final ProcessedRecordCache processedRecordCache,
                               final WatermarkUpdater watermarkUpdater,
                               final OffsetManager offsetManager,
                               final PartitionHelper partitionHelper) {
        super(photonDeserializer, beamCache, beamDataDao, beamDataManifestDao, beamProcessedDao, processedRecordCache, partitionHelper,
                masterManifestConsumer, partitionManifestConsumer, partitionConsumer);
        this.watermarkManager = watermarkManager;
        this.watermarkUpdater = watermarkUpdater;
        this.offsetManager = offsetManager;
    }

    @Override
    public void consume(PhotonBeamReader photonBeamReader, PhotonMessageHandler photonMessageHandler) {
        consume(photonBeamReader, photonMessageHandler, FALSE);
    }

    @Override
    public void consume(PhotonBeamReader photonBeamReader,
                        PhotonMessageHandler photonMessageHandler,
                        Boolean isAsync) {

        PartitionProducer<QueuedMessagesResult, PhotonBeamReader> partitionProducer = new BeamPartitionProducer(beamCache, masterManifestConsumer,
                partitionManifestConsumer, partitionHelper, beamDataManifestDao, beamDataDao, beamProcessedDao, processedRecordCache, FORWARD,
                getStartTime(photonBeamReader.getCurrentWaterMark(), offsetManager.getNowWithOffset()), offsetManager.getNowWithOffset().plusSeconds(5));

        partitionConsumer.init(photonBeamReader.getBeamReaderUuid(), photonBeamReader, partitionProducer);

        BatchResult<QueuedMessagesResult> batchResult;
        do {
            batchResult = partitionConsumer.fetchBatch(photonBeamReader.getBeamReaderUuid());
            if (batchResult.getResults().isPresent()) {

                QueuedMessagesResult queuedMessagesResults = batchResult.getResults().get();

                if (queuedMessagesResults.getQueueMessages().isPresent()) {
                    if (!processResults(photonBeamReader.getBeamReaderUuid(), queuedMessagesResults, photonMessageHandler, photonBeamReader, isAsync)) {
                        return;
                    }
                }

                Instant backoff = watermarkManager.calculateNewWatermark(queuedMessagesResults.getPartitionTime());
                if (backoff.isAfter(photonBeamReader.getCurrentWaterMark())) {
                    // If we are running with an async processor then we send an empty message to keep the watermark moving
                    if (isAsync) {
                        photonMessageHandler.handleMessage(getEmptyPhotonMessage(backoff));
                    }
                    photonBeamReader.setCurrentWaterMark(backoff);
                }
            }

            //Only update the watermark here if we're not using the async processor
            if (!isAsync) {
                photonBeamReader.setWaterMark(photonBeamReader.getCurrentWaterMark());
                watermarkUpdater.updateWatermark(photonBeamReader);
            }
        } while (!batchResult.isLastBatch());
    }

    private boolean processResults(UUID key,
                                   QueuedMessagesResult queuedMessagesResult,
                                   PhotonMessageHandler photonMessageHandler,
                                   PhotonBeamReader photonBeamReader,
                                   Boolean isAsync) {

        try {
            MergeIterator<PhotonRow> entryIterator = new PartitionIterator(getStatefulIterators(queuedMessagesResult.getQueueMessages().get()));

            ProcessedRecordCacheEntry processedRecordCacheEntry = queuedMessagesResult.getProcessedRecordCacheCheck().getProcessedRecordCacheEntry()
                    .orElseGet(() -> translateProcessedResultSetToSet(photonBeamReader.getBeamReaderUuid(),
                            queuedMessagesResult.getPartitionTime(), queuedMessagesResult.getProcessedRecordCacheCheck().getProcessedRecords()));

            if (entryIterator.size() > processedRecordCacheEntry.size()) {

                Optional<PhotonRow> row = entryIterator.getNextEntry();
                if (row.isPresent()) {
                    while (row.isPresent()) {

                        PhotonMessage photonMessage = getPhotonMessage(photonBeamReader, row.get());

                        if (!processedRecordCacheEntry.isPresent(photonMessage.getWriteTime(), photonMessage.getMessageKey())) {
                            if (photonMessage.getWriteTime().isBefore(photonBeamReader.getWaterMark())) {
                                photonMessageHandler.handleStaleMessage(photonMessage);
                            } else {
                                photonMessageHandler.handleMessage(photonMessage);
                            }
                        /* Only persist the processed record if we're not in async mode, otherwise the processed record will be persisted
                        once it has successfully been processed in the async processor */
                            processedRecordCache.putEntry(photonMessage, !isAsync);
                        }

                        row = entryIterator.getNextEntry();
                    }

                    if (queuedMessagesResult.getPartitionTime().isAfter(photonBeamReader.getWaterMark())) {
                        photonBeamReader.setCurrentWaterMark(queuedMessagesResult.getPartitionTime());
                    }
                }
            }
            return true;
        } catch (Exception e) {
            String errorMessage = String.format("Could not process partition: beam = [%s], partition = [%s]",
                    photonBeamReader.getBeamUuid(), queuedMessagesResult.getPartitionTime());
            photonMessageHandler.handleException(new BeamException(errorMessage, e));
            stopConsumers(key);
            return false;
        }
    }

    private Instant getStartTime(Instant currentWaterMark, Instant nowWithOffset) {
        long diff = 15 - currentWaterMark.until(nowWithOffset, ChronoUnit.SECONDS);
        return currentWaterMark.minusSeconds(max(0L, diff));
    }
}
