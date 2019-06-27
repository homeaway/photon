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

import com.google.common.collect.Maps;
import com.homeaway.datatools.photon.api.beam.ConsumerFactory;
import com.homeaway.datatools.photon.api.beam.PhotonConsumer;
import com.homeaway.datatools.photon.api.schema.SchemaClient;
import static com.homeaway.datatools.photon.client.PhotonPropertyConstants.CONSUMER_TYPE;
import static com.homeaway.datatools.photon.client.PhotonPropertyConstants.PHOTON_DESERIALIZER_CLASS;
import static com.homeaway.datatools.photon.client.PhotonPropertyConstants.PHOTON_DRIVER_CLASS;
import static com.homeaway.datatools.photon.client.PhotonPropertyConstants.PHOTON_SCHEMA_CLIENT_CLASS;
import static com.homeaway.datatools.photon.client.PhotonPropertyConstants.WALKBACK_THRESHOLD_MINUTES;
import com.homeaway.datatools.photon.client.cache.BeamCache;
import com.homeaway.datatools.photon.client.cache.BeamReaderCache;
import com.homeaway.datatools.photon.client.cache.DefaultBeamCache;
import com.homeaway.datatools.photon.client.cache.DefaultBeamReaderCache;
import com.homeaway.datatools.photon.client.cache.DefaultProcessedRecordCache;
import com.homeaway.datatools.photon.client.cache.ProcessedRecordCache;
import static com.homeaway.datatools.photon.client.consumer.Consumers.ConsumerType.SINGLE_REGION;
import com.homeaway.datatools.photon.client.consumer.partition.consumer.BeamConsumer;
import com.homeaway.datatools.photon.client.consumer.partition.consumer.DefaultBeamConsumer;
import com.homeaway.datatools.photon.client.consumer.partition.consumer.DefaultWalkBackBeamConsumer;
import com.homeaway.datatools.photon.client.consumer.partition.consumer.WalkBackBeamConsumer;
import com.homeaway.datatools.photon.client.scheduling.DefaultBeamReaderScheduler;
import com.homeaway.datatools.photon.client.scheduling.PhotonScheduler;
import com.homeaway.datatools.photon.client.schema.LocalSchemaClient;
import static com.homeaway.datatools.photon.client.schema.LocalSchemaClient.BEAM_SCHEMA_DAO;
import com.homeaway.datatools.photon.dao.beam.BeamReaderDao;
import com.homeaway.datatools.photon.driver.PhotonDriver;
import static com.homeaway.datatools.photon.serialization.AvroPhotonSerializer.AVRO_SCHEMA_CLIENT;
import com.homeaway.datatools.photon.serialization.PhotonDeserializer;
import static com.homeaway.datatools.photon.utils.client.ClientConstants.BEAM_READ_LOCK_THRESHOLD;
import static com.homeaway.datatools.photon.utils.client.ClientConstants.DEFAULT_WALKBACK_THRESHOLD;
import static com.homeaway.datatools.photon.utils.client.ConsumerUtils.CONSUMER_EXECUTION_FUNCTION;
import com.homeaway.datatools.photon.utils.client.consumer.BeamReaderConfigManager;
import com.homeaway.datatools.photon.utils.client.consumer.BeamReaderLockManager;
import com.homeaway.datatools.photon.utils.client.consumer.DefaultBeamReaderConfigManager;
import com.homeaway.datatools.photon.utils.client.consumer.DefaultBeamReaderLockManager;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

public class Consumers {

    private final PhotonDriver driver;
    private final ConcurrentMap<ConsumerType, ConsumerFactory> factories = Maps.newConcurrentMap();
    private BeamReaderLockManager beamReaderLockManager;
    private BeamReaderConfigManager beamReaderConfigManager;
    private PhotonScheduler beamReaderScheduler;
    private BeamCache beamCache;
    private BeamReaderCache beamReaderCache;
    private BeamReaderDao beamReaderDao;
    private BeamConsumer beamConsumer;
    private ProcessedRecordCache processedRecordCache;
    private WalkBackBeamConsumer walkBackBeamConsumer;
    private Duration walkbackThreshold;

    private static Consumers instance;


    private Consumers(final Properties properties) {
        this.driver = getPhotonDriver(properties);
    }

    private static Consumers getInstance(final Properties properties) {
        return Optional.ofNullable(instance)
                .orElseGet(() -> {
                    instance = new Consumers(properties);
                    return instance;
                });
    }

    private ConsumerFactory getFactory(Properties properties) {
        try {
            PhotonDeserializer photonDeserializer = (PhotonDeserializer) Class.forName(properties.getProperty(PHOTON_DESERIALIZER_CLASS))
                    .getConstructor()
                    .newInstance();

            SchemaClient schemaClient = Optional.ofNullable(properties.getProperty(PHOTON_SCHEMA_CLIENT_CLASS))
                    .map(c -> {
                        try {
                            return (SchemaClient) Class.forName(c).getConstructor().newInstance();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }).orElse(new LocalSchemaClient());

            properties.put(BEAM_SCHEMA_DAO, driver.getBeamSchemaDao());

            schemaClient.configure(properties);

            Map<String, Object> deserializerConfig = Maps.newHashMap();
            deserializerConfig.put(AVRO_SCHEMA_CLIENT, schemaClient);
            photonDeserializer.configure(deserializerConfig);

            processedRecordCache = new DefaultProcessedRecordCache(driver.getBeamProcessedDao());
            walkbackThreshold = Optional.ofNullable(properties.getProperty(WALKBACK_THRESHOLD_MINUTES))
                    .map(t -> Duration.ofMinutes(Integer.parseInt(t))).orElse(DEFAULT_WALKBACK_THRESHOLD);
            beamReaderDao = driver.getBeamReaderDao();
            beamCache = new DefaultBeamCache(driver.getBeamDao());
            beamReaderCache = new DefaultBeamReaderCache(driver.getBeamReaderDao());
            beamReaderConfigManager =
                    new DefaultBeamReaderConfigManager(beamCache, beamReaderCache);
            beamConsumer = new DefaultBeamConsumer(photonDeserializer, beamCache, driver.getBeamDataDao(), driver.getBeamDataManifestDao(),
                    driver.getBeamProcessedDao(), beamReaderDao, processedRecordCache, driver.getPartitionHelper());

            walkBackBeamConsumer = new DefaultWalkBackBeamConsumer(photonDeserializer, beamCache, beamReaderCache, driver.getBeamDataDao(),
                    driver.getBeamDataManifestDao(), driver.getBeamProcessedDao(), driver.getPartitionHelper());

            beamReaderScheduler = new DefaultBeamReaderScheduler(beamReaderConfigManager,
                    beamCache, beamReaderCache, beamConsumer, CONSUMER_EXECUTION_FUNCTION);

            beamReaderLockManager = new DefaultBeamReaderLockManager(beamReaderCache,
                    driver.getBeamReaderLockDao(), BEAM_READ_LOCK_THRESHOLD);

            ConsumerType consumerType = Optional.ofNullable(properties.getProperty(CONSUMER_TYPE))
                    .map(ct -> ConsumerType.valueOf(ct.toUpperCase()))
                    .orElse(SINGLE_REGION);

            return new DefaultConsumerFactory(consumerType.buildConsumer(beamReaderConfigManager,
                    beamReaderScheduler, beamReaderLockManager, beamCache, beamReaderCache, beamReaderDao,
                    beamConsumer, walkBackBeamConsumer, walkbackThreshold), beamCache, beamReaderCache,
                    beamReaderDao, processedRecordCache);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static ConsumerFactory newConsumerFactory(final Properties properties) {
        return getInstance(properties).getFactory(properties);
    }

    public static void shutDown() {
        instance.driver.shutDown();
    }

    enum ConsumerType {

        SINGLE_REGION((beamReaderConfigManager, beamReaderScheduler, beamReaderLockManager, beamCache,
                       beamReaderCache, beamReaderDao, beamConsumer, walkBackBeamConsumer, walkBackThreshold) ->
                new DefaultPhotonConsumer(beamReaderConfigManager, beamReaderScheduler, beamReaderLockManager,
                        beamCache, beamReaderCache, beamReaderDao, beamConsumer)),
        MULTI_REGION((beamReaderConfigManager, beamReaderScheduler, beamReaderLockManager, beamCache,
                      beamReaderCache, beamReaderDao, beamConsumer, walkBackBeamConsumer, walkBackThreshold) ->
                new DefaultPhotonMultiRegionConsumer(beamReaderConfigManager, beamReaderScheduler,
                beamReaderLockManager, beamCache, beamReaderCache, beamReaderDao, beamConsumer, walkBackBeamConsumer,
                Optional.ofNullable(walkBackThreshold).orElse(DEFAULT_WALKBACK_THRESHOLD)));

        private final ConsumerFunction photonConsumerBuilder;

        ConsumerType(final ConsumerFunction photonConsumerBuilder) {
            this.photonConsumerBuilder = photonConsumerBuilder;
        }

        PhotonConsumer buildConsumer(BeamReaderConfigManager beamReaderConfigManager,
                                     PhotonScheduler beamReaderScheduler,
                                     BeamReaderLockManager beamReaderLockManager,
                                     BeamCache beamCache,
                                     BeamReaderCache beamReaderCache,
                                     BeamReaderDao beamReaderDao,
                                     BeamConsumer beamConsumer,
                                     WalkBackBeamConsumer walkBackBeamConsumer,
                                     Duration walkBackThreshold) {
            try {
                return photonConsumerBuilder.build(beamReaderConfigManager, beamReaderScheduler, beamReaderLockManager,
                        beamCache, beamReaderCache, beamReaderDao, beamConsumer, walkBackBeamConsumer, walkBackThreshold);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private PhotonDriver getPhotonDriver(final Properties properties) {
        try {
            return (PhotonDriver) Class.forName(properties.getProperty(PHOTON_DRIVER_CLASS))
                .getConstructor(Properties.class)
                .newInstance(properties);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @FunctionalInterface
    private interface ConsumerFunction {
        PhotonConsumer build(BeamReaderConfigManager beamReaderConfigManager, PhotonScheduler beamReaderScheduler,
                             BeamReaderLockManager beamReaderLockManager, BeamCache beamCache, BeamReaderCache beamReaderCache,
                             BeamReaderDao beamReaderDao, BeamConsumer beamConsumer, WalkBackBeamConsumer walkBackBeamConsumer,
                             Duration walkBackThreshold);
    }
}
