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

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.homeaway.datatools.photon.api.beam.BeamException;
import com.homeaway.datatools.photon.api.beam.PhotonMessage;
import com.homeaway.datatools.photon.api.beam.PhotonMessageHandler;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamDataDao;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamManifestDao;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamProcessedDao;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamReaderDao;
import static com.homeaway.datatools.photon.client.PhotonClientTestHelper.buildPhotonBeam;
import static com.homeaway.datatools.photon.client.PhotonClientTestHelper.buildPhotonBeamReader;
import com.homeaway.datatools.photon.client.cache.BeamCache;
import com.homeaway.datatools.photon.client.cache.DefaultProcessedRecordCache;
import com.homeaway.datatools.photon.client.consumer.partition.consumer.watermark.BaseWatermarkManager;
import com.homeaway.datatools.photon.dao.beam.BeamDataDao;
import com.homeaway.datatools.photon.dao.beam.BeamDataManifestDao;
import com.homeaway.datatools.photon.dao.beam.BeamProcessedDao;
import com.homeaway.datatools.photon.dao.beam.BeamReaderDao;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;
import com.homeaway.datatools.photon.dao.model.message.PhotonProducerMessage;
import com.homeaway.datatools.photon.serialization.JsonPhotonDeserializer;
import static com.homeaway.datatools.photon.utils.client.ClientConstants.PARTITION_SIZE_MILLISECONDS;
import static com.homeaway.datatools.photon.utils.client.ClientConstants.WATERMAKR_MANAGER_WINDOW_UNIT;
import static com.homeaway.datatools.photon.utils.client.ClientConstants.WATERMARK_MANAGER_WINDOW;
import com.homeaway.datatools.photon.utils.consumer.BasePartitionConsumer;
import com.homeaway.datatools.photon.utils.dao.DefaultPartitionHelper;
import com.homeaway.datatools.photon.utils.dao.PartitionHelper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class DefaultBeamConsumerTest {

    private static final Instant START_TIME = Instant.now();
    private PartitionHelper partitionHelper;
    private BeamDataManifestDao beamDataManifestDao;
    private BeamDataDao beamDataDao;
    private BeamReaderDao beamReaderDao;

    @Before
    public void init() {
        partitionHelper = new DefaultPartitionHelper(PARTITION_SIZE_MILLISECONDS);
        beamDataManifestDao = mockBeamManifestDao(partitionHelper);
        beamDataDao = mockBeamDataDao(beamDataManifestDao, partitionHelper);
        beamReaderDao = mockBeamReaderDao();
    }

    @Test
    public void consumeTest() throws InterruptedException {
        PhotonBeam beam = buildPhotonBeam();

        PhotonBeamReader reader = buildPhotonBeamReader();
        reader.setWaterMark(START_TIME.minusSeconds(30));
        reader.setCurrentWaterMark(reader.getWaterMark());
        reader.setBeamUuid(beam.getBeamUuid());

        Queue<PhotonBeam> beamQueue = Lists.newLinkedList();
        beamQueue.add(beam);
        beamQueue.add(buildPhotonBeam());

        BeamCache beamCache = mock(BeamCache.class);
        when(beamCache.getBeamByUuid(beam.getBeamUuid())).thenReturn(Optional.of(beam));
        when(beamCache.getBeamByName(beam.getBeamName())).thenReturn(beamQueue);

        BeamProcessedDao processedDao = mockBeamProcessedDao();
        BeamConsumer beamConsumer = new DefaultBeamConsumer(new JsonPhotonDeserializer(), beamCache, beamDataDao, beamDataManifestDao, processedDao, beamReaderDao,
                new DefaultProcessedRecordCache(processedDao),
                new BasePartitionConsumer<>(5),
                new BasePartitionConsumer<>(10),
                new BasePartitionConsumer<>(50),
                new BaseWatermarkManager(WATERMARK_MANAGER_WINDOW, WATERMAKR_MANAGER_WINDOW_UNIT),
                partitionHelper);

        List<PhotonMessage> messages = Lists.newArrayList();
        List<PhotonMessage> staleMessages = Lists.newArrayList();
        buildTestData(beam.getBeamUuid(), 5, 5, 1, START_TIME.minus(Duration.ofSeconds(5)));
        beamConsumer.consume(reader, new PhotonMessageHandler() {

            @Override
            public void handleMessage(PhotonMessage message) {
                messages.add(message);
            }

            @Override
            public void handleException(BeamException beamException) {

            }

            @Override
            public void handleStaleMessage(PhotonMessage message) {
                staleMessages.add(message);
            }
        });
        Optional<Instant> newWaterMark = beamReaderDao.getWatermark(reader);
        Assert.assertEquals(25, messages.size());
        Assert.assertEquals(0, staleMessages.size());
        Assert.assertTrue(reader.getWaterMark().isAfter(START_TIME.minusSeconds(25)));
        Assert.assertTrue(newWaterMark.isPresent());
        Assert.assertTrue(newWaterMark.get().isAfter(START_TIME.minusSeconds(25)));
    }

    @Test
    public void consumeStaleTest() {
        PhotonBeam beam = buildPhotonBeam();

        PhotonBeamReader reader = buildPhotonBeamReader();
        reader.setWaterMark(partitionHelper.getManifestKey(START_TIME));
        reader.setCurrentWaterMark(reader.getWaterMark());
        reader.setBeamUuid(beam.getBeamUuid());

        Queue<PhotonBeam> beamQueue = Lists.newLinkedList();
        beamQueue.add(beam);
        beamQueue.add(buildPhotonBeam());

        BeamCache beamCache = mock(BeamCache.class);
        when(beamCache.getBeamByUuid(beam.getBeamUuid())).thenReturn(Optional.of(beam));
        when(beamCache.getBeamByName(beam.getBeamName())).thenReturn(beamQueue);

        BeamProcessedDao processedDao = mockBeamProcessedDao();
        BeamConsumer beamConsumer = new DefaultBeamConsumer(new JsonPhotonDeserializer(), beamCache, beamDataDao, beamDataManifestDao, processedDao, mockBeamReaderDao(),
                new DefaultProcessedRecordCache(processedDao),
                new BasePartitionConsumer<>(5),
                new BasePartitionConsumer<>(10),
                new BasePartitionConsumer<>(50),
                new BaseWatermarkManager(WATERMARK_MANAGER_WINDOW, WATERMAKR_MANAGER_WINDOW_UNIT),
                partitionHelper);

        List<PhotonMessage> messages = Lists.newArrayList();
        List<PhotonMessage> staleMessages = Lists.newArrayList();
        buildTestData(beam.getBeamUuid(), 2, 10, 1, START_TIME);
        beamConsumer.consume(reader, new PhotonMessageHandler() {

            @Override
            public void handleMessage(PhotonMessage message) {
                messages.add(message);
            }

            @Override
            public void handleException(BeamException beamException) {

            }

            @Override
            public void handleStaleMessage(PhotonMessage message) {
                staleMessages.add(message);
            }
        });
        Assert.assertTrue(messages.size() > 0);
        Assert.assertEquals(10, staleMessages.size());
    }

    @Test (expected = RuntimeException.class)
    public void consumeExceptionTest() {
        PhotonBeam beam = buildPhotonBeam();

        PhotonBeamReader reader = buildPhotonBeamReader();
        reader.setWaterMark(START_TIME.minusSeconds(25));
        reader.setCurrentWaterMark(reader.getWaterMark());
        reader.setBeamUuid(beam.getBeamUuid());

        Queue<PhotonBeam> beamQueue = Lists.newLinkedList();
        beamQueue.add(beam);
        beamQueue.add(buildPhotonBeam());

        BeamCache beamCache = mock(BeamCache.class);
        when(beamCache.getBeamByUuid(beam.getBeamUuid())).thenReturn(Optional.of(beam));
        when(beamCache.getBeamByName(beam.getBeamName())).thenReturn(beamQueue);

        BeamProcessedDao processedDao = mockBeamProcessedDao();
        BeamConsumer beamConsumer = new DefaultBeamConsumer(new JsonPhotonDeserializer(), beamCache, beamDataDao,
                beamDataManifestDao, processedDao, mockBeamReaderDao(),
                new DefaultProcessedRecordCache(processedDao),
                new BasePartitionConsumer<>(5),
                new BasePartitionConsumer<>(10),
                new BasePartitionConsumer<>(50),
                new BaseWatermarkManager(WATERMARK_MANAGER_WINDOW, WATERMAKR_MANAGER_WINDOW_UNIT),
                partitionHelper);

        buildTestData(beam.getBeamUuid(), 5, 5, 1, START_TIME);
        beamConsumer.consume(reader, new PhotonMessageHandler() {

            @Override
            public void handleMessage(PhotonMessage message) {
                System.out.println(message.getPayload(String.class));
            }

            @Override
            public void handleException(BeamException beamException) {
                throw new RuntimeException(beamException);
            }

            @Override
            public void handleStaleMessage(PhotonMessage message) {

            }
        });
    }

    @Test
    public void consumeHandleExceptionTest() {
        PhotonBeam beam = buildPhotonBeam();

        PhotonBeamReader reader = buildPhotonBeamReader();
        reader.setWaterMark(START_TIME.minusSeconds(25));
        reader.setCurrentWaterMark(reader.getWaterMark());
        reader.setBeamUuid(beam.getBeamUuid());

        Queue<PhotonBeam> beamQueue = Lists.newLinkedList();
        beamQueue.add(beam);
        beamQueue.add(buildPhotonBeam());

        BeamCache beamCache = mock(BeamCache.class);
        when(beamCache.getBeamByUuid(beam.getBeamUuid())).thenReturn(Optional.of(beam));
        when(beamCache.getBeamByName(beam.getBeamName())).thenReturn(beamQueue);

        BeamProcessedDao processedDao = mockBeamProcessedDao();
        BeamConsumer beamConsumer = new DefaultBeamConsumer(new JsonPhotonDeserializer(), beamCache, beamDataDao,
                beamDataManifestDao, processedDao, mockBeamReaderDao(),
                new DefaultProcessedRecordCache(processedDao),
                new BasePartitionConsumer<>(5),
                new BasePartitionConsumer<>(10),
                new BasePartitionConsumer<>(50),
                new BaseWatermarkManager(WATERMARK_MANAGER_WINDOW, WATERMAKR_MANAGER_WINDOW_UNIT),
                partitionHelper);

        buildTestData(beam.getBeamUuid(), 5, 5, 1, START_TIME);
        List<UUID> exceptionThrown = Lists.newArrayList();
        UUID exceptionUuid = UUID.randomUUID();

        beamConsumer.consume(reader, new PhotonMessageHandler() {

            @Override
            public void handleMessage(PhotonMessage message) {
                throw new RuntimeException(exceptionUuid.toString());
            }

            @Override
            public void handleException(BeamException beamException) {
                exceptionThrown.add(UUID.fromString(beamException.getCause().getMessage()));
            }

            @Override
            public void handleStaleMessage(PhotonMessage message) {

            }
        });
        Assert.assertFalse(exceptionThrown.isEmpty());
        Assert.assertEquals(exceptionUuid, exceptionThrown.get(0));
    }

    private void buildTestData(UUID beamUuid, int numManifests, int numPartitions, int numMessagesPerPartition,
                               Instant startTime) {

        for(int i = 0; i < numManifests; i++) {
            Instant manifestTime = partitionHelper.getManifestKey(startTime).minusSeconds(i * 5);
            for (int j = 0; j < numPartitions; j++) {
                Instant partitionTime = manifestTime.plusMillis((5000/numPartitions)*j);
                for (int k = 0; k < numMessagesPerPartition; k++) {
                    try {
                        Futures.allAsList(beamDataDao.putMessageAsync(new PhotonProducerMessage(beamUuid, UUID.randomUUID().toString(),
                                UUID.randomUUID().toString().getBytes(), partitionTime.plusMillis(k)), null)).get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

}
