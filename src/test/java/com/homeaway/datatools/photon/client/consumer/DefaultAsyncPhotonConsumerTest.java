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
package com.homeaway.datatools.photon.client.consumer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.homeaway.datatools.photon.api.beam.AsyncPhotonConsumer;
import com.homeaway.datatools.photon.api.beam.BeamProducer;
import com.homeaway.datatools.photon.api.beam.MessageHandler;
import com.homeaway.datatools.photon.api.beam.PhotonConsumer;
import com.homeaway.datatools.photon.api.schema.SchemaClient;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamDao;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamDataDao;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamManifestDao;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamProcessedDao;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamReaderDao;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamReaderLockDao;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamSchemaDao;
import com.homeaway.datatools.photon.client.cache.BeamCache;
import com.homeaway.datatools.photon.client.cache.BeamReaderCache;
import com.homeaway.datatools.photon.client.cache.DefaultBeamCache;
import com.homeaway.datatools.photon.client.cache.DefaultBeamReaderCache;
import com.homeaway.datatools.photon.client.cache.DefaultProcessedRecordCache;
import com.homeaway.datatools.photon.client.cache.ProcessedRecordCache;
import com.homeaway.datatools.photon.client.consumer.partition.consumer.BeamConsumer;
import com.homeaway.datatools.photon.client.consumer.partition.consumer.DefaultBeamConsumer;
import com.homeaway.datatools.photon.client.consumer.partition.consumer.watermark.BaseOffsetManager;
import com.homeaway.datatools.photon.client.consumer.partition.consumer.watermark.BaseWatermarkManager;
import com.homeaway.datatools.photon.client.consumer.partition.consumer.watermark.DefaultWatermarkUpdater;
import com.homeaway.datatools.photon.client.consumer.partition.consumer.watermark.WatermarkUpdater;
import com.homeaway.datatools.photon.client.producer.DefaultBeamProducer;
import com.homeaway.datatools.photon.client.scheduling.DefaultBeamReaderScheduler;
import com.homeaway.datatools.photon.client.schema.LocalSchemaClient;
import static com.homeaway.datatools.photon.client.schema.LocalSchemaClient.BEAM_SCHEMA_DAO;
import com.homeaway.datatools.photon.dao.beam.BeamDao;
import com.homeaway.datatools.photon.dao.beam.BeamDataDao;
import com.homeaway.datatools.photon.dao.beam.BeamDataManifestDao;
import com.homeaway.datatools.photon.dao.beam.BeamProcessedDao;
import com.homeaway.datatools.photon.dao.beam.BeamReaderDao;
import com.homeaway.datatools.photon.dao.beam.BeamReaderLockDao;
import static com.homeaway.datatools.photon.api.model.enums.PhotonBeamReaderOffsetType.FROM_CURRENT;
import com.homeaway.datatools.photon.serialization.JsonPhotonDeserializer;
import com.homeaway.datatools.photon.serialization.JsonPhotonSerializer;
import static com.homeaway.datatools.photon.utils.client.ClientConstants.BEAM_READ_LOCK_THRESHOLD;
import static com.homeaway.datatools.photon.utils.client.ClientConstants.WATERMAKR_MANAGER_WINDOW_UNIT;
import static com.homeaway.datatools.photon.utils.client.ClientConstants.WATERMARK_MANAGER_WINDOW;
import static com.homeaway.datatools.photon.utils.client.ConsumerUtils.CONSUMER_EXECUTION_FUNCTION;
import com.homeaway.datatools.photon.utils.client.consumer.BeamReaderConfigManager;
import com.homeaway.datatools.photon.utils.client.consumer.BeamReaderLockManager;
import com.homeaway.datatools.photon.utils.client.consumer.DefaultBeamReaderConfigManager;
import com.homeaway.datatools.photon.utils.client.consumer.DefaultBeamReaderLockManager;
import com.homeaway.datatools.photon.utils.consumer.BasePartitionConsumer;
import com.homeaway.datatools.photon.utils.dao.DefaultPartitionHelper;
import com.homeaway.datatools.photon.utils.dao.PartitionHelper;
import com.homeaway.datatools.photon.utils.processing.MessageProcessorException;
import lombok.Data;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;

public class DefaultAsyncPhotonConsumerTest {

    @Test
    public void consumerTest() {
        List<AsyncConsumerEventObject> eventObjects = Lists.newArrayList();
        List<Instant> waterMarks = Lists.newArrayList();
        ConcurrentMap<String, Instant> lastWaterMark = Maps.newConcurrentMap();

        String beamName = "test.beam.name";
        PartitionHelper partitionHelper = new DefaultPartitionHelper(100);
        BeamDao beamDao = mockBeamDao();
        BeamReaderDao beamReaderDao = mockBeamReaderDao();
        BeamCache beamCache = new DefaultBeamCache(beamDao);
        BeamReaderCache beamReaderCache = new DefaultBeamReaderCache(beamReaderDao);
        BeamDataManifestDao beamDataManifestDao = mockBeamManifestDao(partitionHelper);
        BeamDataDao beamDataDao = mockBeamDataDao(beamDataManifestDao, partitionHelper);
        BeamProcessedDao beamProcessedDao = mockBeamProcessedDao();
        ProcessedRecordCache processedRecordCache = new DefaultProcessedRecordCache(beamProcessedDao);
        WatermarkUpdater watermarkUpdater = new DefaultWatermarkUpdater(beamReaderDao);
        BeamReaderLockDao beamReaderLockDao = mockBeamReaderLockDao(BEAM_READ_LOCK_THRESHOLD, Boolean.TRUE);
        Properties properties = new Properties();
        properties.put(BEAM_SCHEMA_DAO, mockBeamSchemaDao());
        SchemaClient schemaClient = new LocalSchemaClient();
        schemaClient.configure(properties);

        BeamConsumer beamConsumer = new DefaultBeamConsumer(new JsonPhotonDeserializer(), beamCache, beamDataDao, beamDataManifestDao,
                beamProcessedDao, new BasePartitionConsumer<>(5),
                new BasePartitionConsumer<>(10),
                new BasePartitionConsumer<>(50),
                new BaseWatermarkManager(WATERMARK_MANAGER_WINDOW, WATERMAKR_MANAGER_WINDOW_UNIT),
                processedRecordCache, watermarkUpdater, new BaseOffsetManager(beamReaderDao), partitionHelper);

        BeamReaderConfigManager beamReaderConfigManager = new DefaultBeamReaderConfigManager(beamCache, beamReaderCache);
        BeamReaderLockManager beamReaderLockManager = new DefaultBeamReaderLockManager(Executors.newScheduledThreadPool(10),
                Executors.newFixedThreadPool(10), beamReaderCache, beamReaderLockDao, Duration.ofMillis(10));

        PhotonConsumer photonConsumer = new DefaultPhotonConsumer(beamReaderConfigManager,
                new DefaultBeamReaderScheduler(beamReaderConfigManager, beamCache, beamReaderCache, beamConsumer, CONSUMER_EXECUTION_FUNCTION),
                beamReaderLockManager, beamCache, beamReaderCache, beamReaderDao, beamConsumer);

        photonConsumer.setPollingInterval(10L);

        AsyncPhotonConsumer<AsyncConsumerEventObject> asyncConsumer =
                new DefaultConsumerFactory(photonConsumer, beamCache, beamReaderCache, beamReaderDao,
                        new DefaultProcessedRecordCache(mockBeamProcessedDao())).getAsyncPhotonConsumer();

        BeamProducer producer = new DefaultBeamProducer(beamDataDao, beamCache, new JsonPhotonSerializer());

        for (int i = 0; i < 50; i++) {
            producer.writeMessageToBeam(beamName, UUID.randomUUID().toString(), UUID.randomUUID().toString());
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        asyncConsumer.putBeamForProcessing("TestClientName", beamName,
                (message) -> {
                    lastWaterMark.put(beamName, message.getWriteTime());
                    AsyncConsumerEventObject object = new AsyncConsumerEventObject();
                    object.setNewMessage("This is a new message for " + message.getPayload(String.class));
                    return object;
                }, new MessageHandler<AsyncConsumerEventObject, MessageProcessorException>() {

                    @Override
                    public void handleMessage(AsyncConsumerEventObject message) {
                        synchronized (eventObjects) {
                            eventObjects.add(message);
                        }
                    }

                    @Override
                    public void handleException(MessageProcessorException exception) {
                        throw new RuntimeException(exception);
                    }

                    @Override
                    public void handleStaleMessage(AsyncConsumerEventObject message) {
                        handleMessage(message);
                    }
                }, FROM_CURRENT);
        try {
            asyncConsumer.start();
            Thread.sleep(5000);
            asyncConsumer.stop();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Assert.assertEquals(50, eventObjects.size());
        Instant prev = null;
        for(Instant waterMark : waterMarks) {
            if (prev == null) {
                prev = waterMark;
            } else {
                Assert.assertTrue(prev.isBefore(waterMark));
                prev = waterMark;
            }
        }
        Assert.assertEquals(beamReaderCache.getPhotonBeamReader("TestClientName", beamCache.getBeamByName(beamName).peek()).get().getPhotonBeamReader().getWaterMark(),
                lastWaterMark.get(beamName));
    }

    @Data
    private static final class AsyncConsumerEventObject {

        String newMessage;

    }
}
