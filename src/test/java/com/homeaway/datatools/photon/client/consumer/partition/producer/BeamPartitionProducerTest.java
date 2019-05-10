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
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.Futures;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamDao;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamDataDao;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamManifestDao;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamProcessedDao;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockPhotonRowSet;
import static com.homeaway.datatools.photon.client.PhotonClientTestHelper.buildPhotonBeam;
import static com.homeaway.datatools.photon.client.PhotonClientTestHelper.buildPhotonBeamReader;
import com.homeaway.datatools.photon.client.cache.BeamCache;
import com.homeaway.datatools.photon.client.cache.DefaultBeamCache;
import com.homeaway.datatools.photon.client.cache.DefaultProcessedRecordCache;
import com.homeaway.datatools.photon.client.cache.ProcessedRecordCache;
import com.homeaway.datatools.photon.dao.beam.BeamDao;
import com.homeaway.datatools.photon.dao.beam.BeamDataDao;
import com.homeaway.datatools.photon.dao.beam.BeamDataManifestDao;
import com.homeaway.datatools.photon.dao.beam.BeamProcessedDao;
import com.homeaway.datatools.photon.dao.beam.WriteFuture;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;
import com.homeaway.datatools.photon.dao.model.message.PhotonProducerMessage;
import com.homeaway.datatools.photon.dao.rows.PhotonRow;
import com.homeaway.datatools.photon.dao.rows.PhotonRowSet;
import com.homeaway.datatools.photon.dao.rows.PhotonRowSetFuture;
import static com.homeaway.datatools.photon.utils.client.ClientConstants.PARTITION_SIZE_MILLISECONDS;
import com.homeaway.datatools.photon.utils.client.PartitionProducerType;
import com.homeaway.datatools.photon.utils.client.QueuedMessagesResult;
import com.homeaway.datatools.photon.utils.consumer.BasePartitionConsumer;
import com.homeaway.datatools.photon.utils.consumer.BatchResult;
import com.homeaway.datatools.photon.utils.consumer.ConsumerQueue;
import com.homeaway.datatools.photon.utils.consumer.PartitionConsumer;
import com.homeaway.datatools.photon.utils.consumer.PartitionProducer;
import static com.homeaway.datatools.photon.utils.dao.Constants.MANIFEST_TIME_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.PARTITION_TIME_COLUMN;
import com.homeaway.datatools.photon.utils.dao.DefaultPartitionHelper;
import com.homeaway.datatools.photon.utils.dao.PartitionHelper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class BeamPartitionProducerTest {

    private static Instant START = Instant.now();
    private ProcessedRecordCache processedRecordCache;
    private PartitionHelper partitionHelper;
    private BeamDao beamDao;
    private BeamCache beamCache;
    private BeamDataManifestDao beamDataManifestDao;
    private BeamDataDao beamDataDao;
    private BeamProcessedDao beamProcessedDao;

    @Before
    public void init() {
        beamDao = mockBeamDao();
        beamCache = new DefaultBeamCache(beamDao);
        partitionHelper = new DefaultPartitionHelper(PARTITION_SIZE_MILLISECONDS);
        beamDataManifestDao = mockBeamManifestDao(partitionHelper);
        beamDataDao = mockBeamDataDao(beamDataManifestDao, partitionHelper);
        beamProcessedDao = mockBeamProcessedDao();
        processedRecordCache = new DefaultProcessedRecordCache(beamProcessedDao);
    }

    @Test
    public void testProducePartitions() throws ExecutionException, InterruptedException {
        PartitionProducer<QueuedMessagesResult, PhotonBeamReader> partitionProducer =
                new BeamPartitionProducer(beamCache,
                        new BasePartitionConsumer<>(5),
                        new BasePartitionConsumer<>(10),
                        partitionHelper,
                        beamDataManifestDao,
                        beamDataDao,
                        beamProcessedDao,
                        processedRecordCache,
                        PartitionProducerType.FORWARD,
                        START,
                        START.plusSeconds(10));

        PhotonBeamReader photonBeamReader = buildPhotonBeamReader();
        PhotonBeam photonBeam = buildPhotonBeam();
        photonBeam.setBeamUuid(photonBeamReader.getBeamUuid());
        beamDao.putBeam(photonBeam);

        for(int i = 0; i < 50; i++) {
            Futures.allAsList(beamDataDao.putMessageAsync(new PhotonProducerMessage(photonBeam.getBeamUuid(), UUID.randomUUID().toString(),
                    UUID.randomUUID().toString().getBytes(), Instant.now().plusMillis(i * PARTITION_SIZE_MILLISECONDS)), null)).get();
        }

        ConcurrentMap<UUID, ConsumerQueue<QueuedMessagesResult>> consumerQueueMap = Maps.newConcurrentMap();
        consumerQueueMap.put(photonBeamReader.getBeamReaderUuid(), new ConsumerQueue<>(Queues.newArrayBlockingQueue(100)));

        ConcurrentMap<UUID, Boolean> producerRunnableFlags = Maps.newConcurrentMap();
        producerRunnableFlags.put(photonBeamReader.getBeamReaderUuid(), true);

        partitionProducer.producePartitionResults(photonBeamReader.getBeamReaderUuid(), consumerQueueMap, photonBeamReader,
                producerRunnableFlags);
        Assert.assertEquals(50, consumerQueueMap.get(photonBeamReader.getBeamReaderUuid()).getBatchResultQueue().size());
        BatchResult<QueuedMessagesResult> result;
        do {
            result = consumerQueueMap.get(photonBeamReader.getBeamReaderUuid()).getBatchResultQueue().poll();
        } while (!result.isLastBatch());
        Assert.assertTrue(result.isLastBatch());
        Assert.assertTrue(consumerQueueMap.get(photonBeamReader.getBeamReaderUuid()).getBatchResultQueue().isEmpty());
    }

    @Test
    public void testProduceNoMasterManifest() {
        PartitionProducer<QueuedMessagesResult, PhotonBeamReader> partitionProducer =
                new BeamPartitionProducer(beamCache,
                        new BasePartitionConsumer<>(5),
                        new BasePartitionConsumer<>(10),
                        partitionHelper,
                        beamDataManifestDao,
                        beamDataDao,
                        beamProcessedDao,
                        processedRecordCache,
                        PartitionProducerType.FORWARD,
                        START, START.plusSeconds(10));

        PhotonBeamReader photonBeamReader = buildPhotonBeamReader();
        PhotonBeam photonBeam = buildPhotonBeam();
        photonBeam.setBeamUuid(photonBeamReader.getBeamUuid());
        beamDao.putBeam(photonBeam);

        ConcurrentMap<UUID, ConsumerQueue<QueuedMessagesResult>> consumerQueueMap = Maps.newConcurrentMap();
        consumerQueueMap.put(photonBeamReader.getBeamReaderUuid(), new ConsumerQueue<>(Queues.newArrayBlockingQueue(100)));

        ConcurrentMap<UUID, Boolean> producerRunnableFlags = Maps.newConcurrentMap();
        producerRunnableFlags.put(photonBeamReader.getBeamReaderUuid(), true);

        partitionProducer.producePartitionResults(photonBeamReader.getBeamReaderUuid(), consumerQueueMap, photonBeamReader,
                producerRunnableFlags);
        Assert.assertEquals(1, consumerQueueMap.get(photonBeamReader.getBeamReaderUuid()).getBatchResultQueue().size());
        BatchResult<QueuedMessagesResult> result;
        do {
            result = consumerQueueMap.get(photonBeamReader.getBeamReaderUuid()).getBatchResultQueue().poll();
        } while (!result.isLastBatch());
        Assert.assertTrue(result.isLastBatch());
        Assert.assertTrue(consumerQueueMap.get(photonBeamReader.getBeamReaderUuid()).getBatchResultQueue().isEmpty());
    }

    @Test
    public void testShutDownProducer() throws ExecutionException, InterruptedException {
        PartitionProducer<QueuedMessagesResult, PhotonBeamReader> partitionProducer =
                new BeamPartitionProducer(mock(BeamCache.class),
                        new BasePartitionConsumer<>(5),
                        new BasePartitionConsumer<>(10),
                        partitionHelper,
                        beamDataManifestDao,
                        beamDataDao,
                        beamProcessedDao,
                        processedRecordCache,
                        PartitionProducerType.FORWARD,
                        START,
                        START.plusSeconds(10));

        PhotonBeamReader photonBeamReader = buildPhotonBeamReader();
        PhotonBeam photonBeam = buildPhotonBeam();
        photonBeam.setBeamUuid(photonBeamReader.getBeamUuid());
        beamDao.putBeam(photonBeam);

        for(int i = 0; i < 50; i++) {
            Futures.allAsList(beamDataDao.putMessageAsync(new PhotonProducerMessage(photonBeam.getBeamUuid(), UUID.randomUUID().toString(),
                    UUID.randomUUID().toString().getBytes(), Instant.now().plusMillis(i * PARTITION_SIZE_MILLISECONDS)), null)).get();
        }

        ConcurrentMap<UUID, ConsumerQueue<QueuedMessagesResult>> consumerQueueMap = Maps.newConcurrentMap();
        consumerQueueMap.put(photonBeamReader.getBeamReaderUuid(), new ConsumerQueue<>(Queues.newArrayBlockingQueue(100)));

        ConcurrentMap<UUID, Boolean> producerRunnableFlags = Maps.newConcurrentMap();
        producerRunnableFlags.put(photonBeamReader.getBeamReaderUuid(), false);

        partitionProducer.producePartitionResults(photonBeamReader.getBeamReaderUuid(), consumerQueueMap, photonBeamReader,
                producerRunnableFlags);

        Assert.assertEquals(1, consumerQueueMap.get(photonBeamReader.getBeamReaderUuid()).getBatchResultQueue().size());
        BatchResult<QueuedMessagesResult> result = consumerQueueMap.get(photonBeamReader.getBeamReaderUuid()).getBatchResultQueue().poll();
        Assert.assertTrue(result.isLastBatch());
        Assert.assertFalse(result.getResults().isPresent());
    }

    @Test
    public void testExceptionInProcessing() {
        PhotonBeamReader photonBeamReader = buildPhotonBeamReader();

        PhotonBeam beam = buildPhotonBeam();
        beam.setBeamUuid(photonBeamReader.getBeamUuid());

        BeamCache beamCache = mock(BeamCache.class);
        when(beamCache.getBeamByName(anyString())).thenReturn(Lists.newLinkedList(Collections.singletonList(beam)));
        when(beamCache.getBeamByUuid(any(UUID.class))).thenReturn(Optional.of(beam));

        PartitionConsumer<PartitionManifestResult, PhotonBeamReader> masterManifestConsumer = new BasePartitionConsumer<>(1);
        PartitionConsumer<PartitionManifestResult, PhotonBeamReader> manifestPartitionConsumer = new BasePartitionConsumer<>(1);

        ConcurrentMap<UUID, ConsumerQueue<QueuedMessagesResult>> consumerQueueMap = Maps.newConcurrentMap();
        consumerQueueMap.put(photonBeamReader.getBeamReaderUuid(), new ConsumerQueue<>(Queues.newArrayBlockingQueue(100)));

        ConcurrentMap<UUID, Boolean> producerRunnableFlags = Maps.newConcurrentMap();
        producerRunnableFlags.put(photonBeamReader.getBeamReaderUuid(), true);

        ProcessedRecordCache processedRecordCache = mock(ProcessedRecordCache.class);
        when(processedRecordCache.getProcessedEntities(any(PhotonBeamReader.class), any(Instant.class)))
                .thenReturn(Optional.empty());

        PartitionProducer<QueuedMessagesResult, PhotonBeamReader> partitionProducer =
                new BeamPartitionProducer(beamCache,
                        masterManifestConsumer,
                        manifestPartitionConsumer,
                        new DefaultPartitionHelper(PARTITION_SIZE_MILLISECONDS),
                        new MockedManifestDaoFailure(),
                        mock(BeamDataDao.class),
                        mock(BeamProcessedDao.class),
                        processedRecordCache,
                        PartitionProducerType.FORWARD,
                        START,
                        START.plusSeconds(10));

        partitionProducer.producePartitionResults(photonBeamReader.getBeamReaderUuid(), consumerQueueMap, photonBeamReader,
                producerRunnableFlags);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Assert.assertEquals(1, consumerQueueMap.get(photonBeamReader.getBeamReaderUuid()).getBatchResultQueue().size());
        Assert.assertTrue(consumerQueueMap.get(photonBeamReader.getBeamReaderUuid()).getBatchResultQueue().peek().isLastBatch());
    }

    private class MockedManifestDaoFailure implements BeamDataManifestDao {

        private int callCount = 0;

        @Override
        public PhotonRowSetFuture getBeamMasterManifest(UUID beamUuid, Instant masterManifestTime, Instant minManifestTime, Instant maxManifestTime) {
            Queue<PhotonRow> rows = Lists.newLinkedList();
            for (int i = 0; i < 5; i++) {
                PhotonRow photonRow = mock(PhotonRow.class);
                when(photonRow.getInstant(MANIFEST_TIME_COLUMN)).thenReturn(Instant.now());
                 rows.add(photonRow);
            }
            PhotonRowSet rowSet = mockPhotonRowSet(rows);
            PhotonRowSetFuture future = mock(PhotonRowSetFuture.class);
            try {
                when(future.getUninterruptibly(anyInt(), any(TimeUnit.class))).thenReturn(rowSet);
            } catch (TimeoutException e) {
                throw new RuntimeException(e);
            }
            return future;
        }

        @Override
        public WriteFuture putBeamMasterManifest(UUID beamUuid, Instant writeTime, Integer messageTtl) {
            return null;
        }

        @Override
        public PhotonRowSetFuture getBeamDataManifest(UUID beamUuid, Instant manifestTime, Instant maxPartitionTime) {
            PhotonRowSetFuture photonRowSetFuture = mock(PhotonRowSetFuture.class);
            Queue<PhotonRow> rows = Lists.newLinkedList();
            if (callCount > 0) {
                try {
                    doThrow(TimeoutException.class).when(photonRowSetFuture).getUninterruptibly(any(Integer.class), any(TimeUnit.class));
                } catch (TimeoutException e) {
                    throw new RuntimeException(e);
                }
            } else {
                for (int j = 0; j < 5; j++) {
                    PhotonRow row = mock(PhotonRow.class);
                    when(row.getInstant(PARTITION_TIME_COLUMN)).thenReturn(manifestTime.plusSeconds(j));
                    rows.add(row);
                }
                try {
                    when(photonRowSetFuture.getUninterruptibly(any(Integer.class), any(TimeUnit.class))).thenReturn(mockPhotonRowSet(rows));
                } catch (TimeoutException e) {
                    throw new RuntimeException(e);
                }
            }
            callCount++;
            return photonRowSetFuture;
        }

        @Override
        public WriteFuture putBeamDataManifest(UUID beamUuid, Instant writeTime, Integer messageTtl) {
            return null;
        }
    }
}
