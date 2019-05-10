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
package com.homeaway.datatools.photon.client.schema;

import com.google.common.collect.Maps;
import com.homeaway.datatools.photon.api.schema.SchemaClient;
import com.homeaway.datatools.photon.api.schema.SchemaException;
import com.homeaway.datatools.photon.dao.beam.BeamSchemaDao;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import org.apache.avro.Schema;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.SchemaValidator;
import org.apache.avro.SchemaValidatorBuilder;

import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

public class LocalSchemaClient implements SchemaClient {

    public static final String BEAM_SCHEMA_DAO = "beam.schema.dao";
    private final SchemaValidator schemaValidator;
    private final ConcurrentMap<UUID, Schema> schemaCache;
    private BeamSchemaDao beamSchemaDao;

    public LocalSchemaClient() {
        this(new SchemaValidatorBuilder().canReadStrategy().validateLatest(), Maps.newConcurrentMap());
    }

    public LocalSchemaClient(final SchemaValidator schemaValidator,
                             final ConcurrentMap<UUID, Schema> schemaCache) {
        this.schemaValidator = schemaValidator;
        this.schemaCache = schemaCache;
    }

    @Override
    public void configure(Properties properties) {
        this.beamSchemaDao = (BeamSchemaDao)properties.get(BEAM_SCHEMA_DAO);
    }

    @Override
    public Schema getSchema(PhotonBeam beam) {
        return schemaCache.computeIfAbsent(beam.getBeamUuid(),
                u -> beamSchemaDao.getSchemaByBeamUuid(u)
                        .orElse(null));
    }

    @Override
    public void registerSchema(PhotonBeam beam, Schema schema) throws SchemaException {
        Schema currentSchema = schemaCache.computeIfAbsent(beam.getBeamUuid(), u -> beamSchemaDao.getSchemaByBeamUuid(u)
                .orElseGet(() -> {
                    beamSchemaDao.putBeamSchema(beam, schema);
                    return schema;
                }));

        if (!currentSchema.equals(schema)) {
            try {
                schemaValidator.validate(schema, Collections.singletonList(currentSchema));
                beamSchemaDao.putBeamSchema(beam, schema);
            } catch (SchemaValidationException e) {
                throw new SchemaException("Schema is not compatible with existing schema", e);
            }
        }
    }
}
