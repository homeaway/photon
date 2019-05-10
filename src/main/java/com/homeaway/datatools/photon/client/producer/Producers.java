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
package com.homeaway.datatools.photon.client.producer;

import com.google.common.collect.Maps;
import com.homeaway.datatools.photon.api.beam.BeamProducer;
import com.homeaway.datatools.photon.api.schema.SchemaClient;
import static com.homeaway.datatools.photon.client.PhotonPropertyConstants.PHOTON_DRIVER_CLASS;
import static com.homeaway.datatools.photon.client.PhotonPropertyConstants.PHOTON_SCHEMA_CLIENT_CLASS;
import static com.homeaway.datatools.photon.client.PhotonPropertyConstants.PHOTON_SERIALIZER_CLASS;
import com.homeaway.datatools.photon.client.cache.BeamCache;
import com.homeaway.datatools.photon.client.cache.DefaultBeamCache;
import com.homeaway.datatools.photon.client.schema.LocalSchemaClient;
import static com.homeaway.datatools.photon.client.schema.LocalSchemaClient.BEAM_SCHEMA_DAO;
import com.homeaway.datatools.photon.dao.beam.BeamDao;
import com.homeaway.datatools.photon.dao.beam.BeamDataDao;
import com.homeaway.datatools.photon.driver.PhotonDriver;
import static com.homeaway.datatools.photon.serialization.AvroPhotonSerializer.AVRO_SCHEMA_CLIENT;
import com.homeaway.datatools.photon.serialization.PhotonSerializer;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class Producers {

    public static BeamProducer newProducer(Properties properties) {
        try {
            PhotonDriver driver = (PhotonDriver)Class.forName(properties.getProperty(PHOTON_DRIVER_CLASS))
                    .getConstructor(Properties.class)
                    .newInstance(properties);

            PhotonSerializer photonSerializer = (PhotonSerializer)Class.forName(properties.getProperty(PHOTON_SERIALIZER_CLASS))
                    .getConstructor()
                    .newInstance();

            SchemaClient schemaClient = Optional.ofNullable(properties.getProperty(PHOTON_SCHEMA_CLIENT_CLASS))
                    .map(c -> {
                        try {
                            return (SchemaClient)Class.forName(c).getConstructor().newInstance();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }).orElse(new LocalSchemaClient());

            properties.put(BEAM_SCHEMA_DAO, driver.getBeamDataDao());

            return newProducer(driver, photonSerializer, schemaClient);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static BeamProducer newProducer(PhotonDriver driver,
                                            PhotonSerializer photonSerializer,
                                            SchemaClient schemaClient) {
        Map<String, Object> serializerConfig = Maps.newHashMap();
        serializerConfig.put(AVRO_SCHEMA_CLIENT, schemaClient);
        photonSerializer.configure(serializerConfig);
        return newProducer(driver, photonSerializer);
    }


    private static BeamProducer newProducer(PhotonDriver driver,
                                            PhotonSerializer photonSerializer) {
        final BeamDataDao beamDataDao = driver.getBeamDataDao();
        final BeamDao beamDao = driver.getBeamDao();
        final BeamCache beamCache = new DefaultBeamCache(beamDao);
        return new DefaultBeamProducer(beamDataDao, beamCache, photonSerializer);
    }
}
