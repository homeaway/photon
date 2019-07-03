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
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamSchemaDao;
import com.homeaway.datatools.photon.api.beam.BeamException;
import com.homeaway.datatools.photon.api.beam.BeamProducer;
import com.homeaway.datatools.photon.api.beam.PhotonConsumer;
import com.homeaway.datatools.photon.api.beam.PhotonMessage;
import com.homeaway.datatools.photon.api.beam.PhotonMessageHandler;
import com.homeaway.datatools.photon.api.schema.SchemaClient;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamDao;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamDataDao;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamManifestDao;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamProcessedDao;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamReaderDao;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamReaderLockDao;
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
import com.homeaway.datatools.photon.api.model.enums.PhotonBeamReaderOffsetType;
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
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

@Slf4j
public class DefaultPhotonConsumerTest {

    private PartitionHelper partitionHelper;
    private BeamDao beamDao;
    private BeamReaderDao beamReaderDao;
    private BeamDataManifestDao beamDataManifestDao;
    private BeamProcessedDao beamProcessedDao;
    private BeamDataDao beamDataDao;
    private BeamCache beamCache;
    private BeamReaderCache beamReaderCache;
    private ProcessedRecordCache processedRecordCache;
    private WatermarkUpdater watermarkUpdater;
    private BeamReaderLockDao beamReaderLockDao;
    private SchemaClient schemaClient;

    @Before
    public void init() {
        partitionHelper = new DefaultPartitionHelper(100);
        beamDao = mockBeamDao();
        beamReaderDao = mockBeamReaderDao();
        beamCache = new DefaultBeamCache(beamDao);
        beamReaderCache = new DefaultBeamReaderCache(beamReaderDao);
        beamDataManifestDao = mockBeamManifestDao(partitionHelper);
        beamDataDao = mockBeamDataDao(beamDataManifestDao, partitionHelper);
        beamProcessedDao = mockBeamProcessedDao();
        processedRecordCache = new DefaultProcessedRecordCache(beamProcessedDao);
        watermarkUpdater = new DefaultWatermarkUpdater(beamReaderDao);
        beamReaderLockDao = mockBeamReaderLockDao(BEAM_READ_LOCK_THRESHOLD, Boolean.TRUE);
        Properties properties = new Properties();
        properties.put(BEAM_SCHEMA_DAO, mockBeamSchemaDao());
        schemaClient = new LocalSchemaClient();
        schemaClient.configure(properties);
    }

    @Test
    public void consumerTest() {
        List<PhotonMessage> messages = Lists.newArrayList();
        String beamName = "test.beam.name";

        BeamConsumer beamConsumer = new DefaultBeamConsumer(new JsonPhotonDeserializer(), beamCache, beamDataDao, beamDataManifestDao,
                beamProcessedDao, new BasePartitionConsumer<>(5), new BasePartitionConsumer<>(10), new BasePartitionConsumer<>(50),
                new BaseWatermarkManager(WATERMARK_MANAGER_WINDOW, WATERMAKR_MANAGER_WINDOW_UNIT),
                processedRecordCache, watermarkUpdater, new BaseOffsetManager(beamReaderDao), partitionHelper);

        BeamReaderConfigManager beamReaderConfigManager = new DefaultBeamReaderConfigManager(beamCache, beamReaderCache);
        BeamReaderLockManager beamReaderLockManager = new DefaultBeamReaderLockManager(beamReaderCache, beamReaderLockDao, BEAM_READ_LOCK_THRESHOLD);

        PhotonConsumer photonConsumer = new DefaultPhotonConsumer(beamReaderConfigManager,
                new DefaultBeamReaderScheduler(beamReaderConfigManager, beamCache, beamReaderCache, beamConsumer, CONSUMER_EXECUTION_FUNCTION),
                beamReaderLockManager, beamCache, beamReaderCache, beamReaderDao, beamConsumer);

        photonConsumer.setPollingInterval(50L);

        BeamProducer producer = new DefaultBeamProducer(beamDataDao, beamCache, new JsonPhotonSerializer());

        for (int i = 0; i < 50; i++) {
            producer.writeMessageToBeam(beamName, UUID.randomUUID().toString(), UUID.randomUUID());
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        photonConsumer.putBeamForProcessing("TestClientName", beamName,
                new PhotonMessageHandler() {
                    @Override
                    public void handleMessage(PhotonMessage message) {
                        messages.add(message);
                    }

                    @Override
                    public void handleException(BeamException beamException) {

                    }

                    @Override
                    public void handleStaleMessage(PhotonMessage message) {
                        handleMessage(message);
                    }
                }, PhotonBeamReaderOffsetType.FROM_BEGINNING);
        try {
            photonConsumer.start();
            Thread.sleep(1000);
            photonConsumer.stop();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Assert.assertEquals(50, messages.size());
    }

    @Test
    public void consumerSparseDataTest() {
        List<PhotonMessage> messages = Lists.newArrayList();
        String beamName = "test.beam.name";
        Instant start = Instant.now().minus(30, ChronoUnit.MINUTES);

        BeamConsumer beamConsumer = new DefaultBeamConsumer(new JsonPhotonDeserializer(), beamCache, beamDataDao, beamDataManifestDao,
                beamProcessedDao, new BasePartitionConsumer<>(5), new BasePartitionConsumer<>(10), new BasePartitionConsumer<>(50),
                new BaseWatermarkManager(WATERMARK_MANAGER_WINDOW, WATERMAKR_MANAGER_WINDOW_UNIT),
                processedRecordCache, watermarkUpdater, new BaseOffsetManager(beamReaderDao), partitionHelper);

        BeamReaderConfigManager beamReaderConfigManager = new DefaultBeamReaderConfigManager(beamCache, beamReaderCache);
        BeamReaderLockManager beamReaderLockManager = new DefaultBeamReaderLockManager(beamReaderCache, beamReaderLockDao, BEAM_READ_LOCK_THRESHOLD);

        PhotonConsumer photonConsumer = new DefaultPhotonConsumer(beamReaderConfigManager,
                new DefaultBeamReaderScheduler(beamReaderConfigManager, beamCache, beamReaderCache, beamConsumer, CONSUMER_EXECUTION_FUNCTION),
                beamReaderLockManager, beamCache, beamReaderCache, beamReaderDao, beamConsumer);

        photonConsumer.setPollingInterval(50L);

        BeamProducer producer = new DefaultBeamProducer(beamDataDao, beamCache, new JsonPhotonSerializer());

        for (int i = 0; i < 10; i++) {
            producer.writeMessageToBeam(beamName, UUID.randomUUID().toString(), UUID.randomUUID(), start.plus(i, ChronoUnit.MINUTES));
        }


        photonConsumer.putBeamForProcessing("TestClientName", beamName,
                new PhotonMessageHandler() {
                    @Override
                    public void handleMessage(PhotonMessage message) {
                        messages.add(message);
                    }

                    @Override
                    public void handleException(BeamException beamException) {

                    }

                    @Override
                    public void handleStaleMessage(PhotonMessage message) {

                    }
                }, PhotonBeamReaderOffsetType.FROM_OFFSET, (client, beam) -> start);
        try {
            photonConsumer.start();
            Thread.sleep(500);
            photonConsumer.stop();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Assert.assertEquals(10, messages.size());
    }
}
