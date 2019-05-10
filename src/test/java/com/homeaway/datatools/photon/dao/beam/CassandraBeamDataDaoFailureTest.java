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
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.google.common.util.concurrent.Futures;
import com.homeaway.datatools.photon.dao.beam.cassandra.CassandraBeamDataDao;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;
import com.homeaway.datatools.photon.dao.model.message.PhotonProducerMessage;
import com.homeaway.datatools.photon.utils.dao.DefaultPartitionHelper;
import com.homeaway.datatools.photon.utils.dao.PartitionHelper;
import com.homeaway.datatools.photon.utils.test.cassandra.testing.EmbeddedCassandraRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class CassandraBeamDataDaoFailureTest {

    private BeamDataDao beamDataDao;
    private PartitionHelper partitionHelper;
    private Session session;

    @Rule
    public EmbeddedCassandraRule embeddedCassandraRule = new EmbeddedCassandraRule("cassandra/create_keyspace.cql",
            "cassandra/beam_data.cql", "cassandra/beam_data_manifest.cql", "cassandra/beam_data_master_manifest.cql");

    @Before
    public void init() {
        this.session = embeddedCassandraRule.cluster().connect("photon");
        this.partitionHelper = new DefaultPartitionHelper(100);
        this.beamDataDao = new CassandraBeamDataDao(session, partitionHelper);
    }

    @Test (expected = ExecutionException.class)
    public void testPutMessageFailureNoManifest() throws InterruptedException, ExecutionException {
        dropManifestTable();
        PhotonBeam beam = CassandraBeamDaoTestHelper.buildPhotonBeam();
        PhotonBeamReader reader = new PhotonBeamReader();
        reader.setBeamUuid(beam.getBeamUuid());
        PhotonProducerMessage message = new PhotonProducerMessage(beam.getBeamUuid(), "TestMessageKeyManifest", UUID.randomUUID().toString().getBytes(), Instant.now());
        Futures.allAsList(beamDataDao.putMessageAsync(message, beam.getDefaultTtl())).get();
    }

    @Test (expected = ExecutionException.class)
    public void testPutMessageFailureNoData() throws InterruptedException, ExecutionException {
        dropDataTable();
        PhotonBeam beam = CassandraBeamDaoTestHelper.buildPhotonBeam();
        PhotonBeamReader reader = new PhotonBeamReader();
        reader.setBeamUuid(beam.getBeamUuid());
        PhotonProducerMessage message = new PhotonProducerMessage(beam.getBeamUuid(), "TestMessageKeyData", UUID.randomUUID().toString().getBytes(), Instant.now());
        Futures.allAsList(beamDataDao.putMessageAsync(message, beam.getDefaultTtl())).get();
    }

    private void dropManifestTable() {
        Statement drop = SchemaBuilder.dropTable("beam_data_manifest");
        session.execute(drop);
    }

    private void dropDataTable() {
        Statement drop = SchemaBuilder.dropTable("beam_data");
        session.execute(drop);
    }
}
