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
import com.homeaway.datatools.photon.dao.beam.cassandra.CassandraBeamDao;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import com.homeaway.datatools.photon.utils.test.cassandra.testing.EmbeddedCassandraRule;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;

public class CassandraBeamDaoTest {

    private BeamDao beamDao;

    @Rule
    public EmbeddedCassandraRule embeddedCassandraRule = new EmbeddedCassandraRule("cassandra/create_keyspace.cql",
            "cassandra/beam_by_name.cql", "cassandra/beam_by_uuid.cql");

    @Before
    public void init() {
        Session session = embeddedCassandraRule.cluster().connect("photon");
        session.execute("TRUNCATE TABLE beam_by_name;");
        session.execute("TRUNCATE TABLE beam_by_uuid;");
        beamDao = new CassandraBeamDao(session);
    }

    @Test
    public void testPutBeam() {
        UUID beamUuid = UUID.randomUUID();
        String beamName = "TestNewBeam";
        Instant beamStart = Instant.ofEpochMilli(Instant.now().toEpochMilli());
        PhotonBeam beam = new PhotonBeam();
        beam.setBeamUuid(beamUuid);
        beam.setBeamName(beamName);
        beam.setStartDate(beamStart);
        beamDao.putBeam(beam);

        Queue<PhotonBeam> testBeamName = beamDao.getBeamByName(beamName);
        Optional<PhotonBeam> testBeamUuid = beamDao.getBeamByUuid(beamUuid);
        Assert.assertFalse(testBeamName.isEmpty());
        Assert.assertTrue(testBeamUuid.isPresent());
        assertBeamsEqualNoTtl(beam, testBeamName.peek());
        assertBeamsEqualNoTtl(beam, testBeamUuid.get());
    }

    @Test
    public void testGetBeamByName() {
        UUID beamUuid = UUID.randomUUID();
        String beamName = "TestBeamByName";
        Instant beamStart = Instant.ofEpochMilli(Instant.now().toEpochMilli());
        PhotonBeam beam = new PhotonBeam();
        beam.setBeamUuid(beamUuid);
        beam.setBeamName(beamName);
        beam.setStartDate(beamStart);
        beamDao.putBeam(beam);

        PhotonBeam testBeamName = beamDao.getBeamByName(beamName).peek();
        Assert.assertNotNull(testBeamName);
        assertBeamsEqualNoTtl(beam, testBeamName);
    }

    @Test
    public void testGetBeamByUuid() {
        UUID beamUuid = UUID.randomUUID();
        String beamName = "TestBeamByUuid";
        Instant beamStart = Instant.ofEpochMilli(Instant.now().toEpochMilli());
        PhotonBeam beam = new PhotonBeam();
        beam.setBeamUuid(beamUuid);
        beam.setBeamName(beamName);
        beam.setStartDate(beamStart);
        beamDao.putBeam(beam);

        Optional<PhotonBeam> testBeamUuid = beamDao.getBeamByUuid(beamUuid);
        Assert.assertTrue(testBeamUuid.isPresent());
        assertBeamsEqualNoTtl(beam, testBeamUuid.get());
    }

    @Test
    public void testGetBeamByNameNotFound() {
        Queue<PhotonBeam> beams = beamDao.getBeamByName("BeamNotFound");
        Assert.assertNotNull(beams);
        Assert.assertTrue(beams.isEmpty());
    }

    private void assertBeamsEqualNoTtl(PhotonBeam beam_a, PhotonBeam beam_b) {
        Assert.assertEquals(beam_a.getBeamUuid(), beam_b.getBeamUuid());
        Assert.assertEquals(beam_a.getBeamName(), beam_b.getBeamName());
        Assert.assertEquals(beam_a.getStartDate(), beam_b.getStartDate());
        Assert.assertEquals(0, beam_b.getDefaultTtl().intValue());

    }
}
