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
package com.homeaway.datatools.photon.dao.beam;

import com.datastax.driver.core.Session;
import com.homeaway.datatools.photon.dao.beam.cassandra.CassandraBeamSchemaDao;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import com.homeaway.datatools.photon.utils.test.cassandra.testing.EmbeddedCassandraRule;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Optional;
import java.util.UUID;

public class CassandraBeamSchemaDaoTest {

    private BeamSchemaDao beamSchemaDao;

    @Rule
    public EmbeddedCassandraRule embeddedCassandraRule = new EmbeddedCassandraRule("cassandra/create_keyspace.cql",
            "cassandra/beam_schema.cql");

    @Before
    public void init() {
        Session session = embeddedCassandraRule.cluster().connect("photon");
        beamSchemaDao = new CassandraBeamSchemaDao(session);
    }

    @Test
    public void testPutGetBeamSchema() {
        Schema schema = ReflectData.get().getSchema(String.class);
        PhotonBeam beam = new PhotonBeam();
        beam.setBeamUuid(UUID.randomUUID());
        beamSchemaDao.putBeamSchema(beam, schema).getUninterruptibly();
        Optional<Schema> newSchema = beamSchemaDao.getSchemaByBeamUuid(beam.getBeamUuid());
        Assert.assertTrue(newSchema.isPresent());
        Assert.assertEquals(schema, newSchema.get());
    }
}
