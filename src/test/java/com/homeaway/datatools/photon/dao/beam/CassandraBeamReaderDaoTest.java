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
import com.homeaway.datatools.photon.dao.beam.cassandra.CassandraBeamReaderDao;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;
import com.homeaway.datatools.photon.utils.test.cassandra.testing.EmbeddedCassandraRule;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.UUID;

public class CassandraBeamReaderDaoTest {

    private BeamReaderDao beamReaderDao;

    @Rule
    public EmbeddedCassandraRule embeddedCassandraRule = new EmbeddedCassandraRule("cassandra/create_keyspace.cql",
            "cassandra/beam_reader.cql");

    @Before
    public void init() {
        Session session = embeddedCassandraRule.cluster().connect("photon");
        this.beamReaderDao = new CassandraBeamReaderDao(session);
    }

    @Test
    public void testPutGetBeamReader() {
        String clientName = "TestService";
        UUID beamUuid = UUID.randomUUID();
        PhotonBeamReader beamReader = new PhotonBeamReader();
        beamReader.setBeamUuid(beamUuid);
        beamReader.setBeamReaderUuid(UUID.randomUUID());
        beamReader.setClientName(clientName);
        beamReader.setWaterMark(Instant.ofEpochMilli(Instant.now().toEpochMilli()));

        beamReaderDao.putBeamReader(beamReader);
        Optional<PhotonBeamReader> beamReaderTest = beamReaderDao.getBeamReaderByClientNameBeamUuid(clientName, beamUuid);
        Assert.assertTrue(beamReaderTest.isPresent());
        Assert.assertNull(beamReaderTest.get().getWaterMark());
        Assert.assertFalse(beamReaderTest.get().getPhotonBeamReaderLock().isPresent());

        beamReader.setBeamReaderUuid(UUID.randomUUID());
        beamReaderDao.putBeamReader(beamReader);
        beamReaderTest = beamReaderDao.getBeamReaderByClientNameBeamUuid(clientName, beamUuid);
        Assert.assertTrue(beamReaderTest.isPresent());
        Assert.assertNull(beamReaderTest.get().getWaterMark());
        Assert.assertFalse(beamReaderTest.get().getPhotonBeamReaderLock().isPresent());
    }

    @Test
    public void testUpdateWatermark() {
        String clientName = "TestService";
        UUID beamUuid = UUID.randomUUID();
        PhotonBeamReader beamReader = new PhotonBeamReader();
        beamReader.setBeamUuid(beamUuid);
        beamReader.setBeamReaderUuid(UUID.randomUUID());
        beamReader.setClientName(clientName);
        beamReader.setWaterMark(Instant.ofEpochMilli(Instant.now().toEpochMilli()));

        beamReaderDao.putBeamReader(beamReader);
        Optional<Instant> watermark = beamReaderDao.getWatermark(beamReader);
        Assert.assertFalse(watermark.isPresent());

        beamReader.setWaterMark(Instant.ofEpochMilli(Instant.now().toEpochMilli()));
        beamReaderDao.updateWatermark(beamReader).getUninterruptibly();

        watermark = beamReaderDao.getWatermark(beamReader);
        Assert.assertTrue(watermark.isPresent());
        Assert.assertEquals(beamReader.getWaterMark(), watermark.get());

        beamReader.setWaterMark(Instant.ofEpochMilli(Instant.now().toEpochMilli()).plus(100, ChronoUnit.MILLIS));
        beamReaderDao.updateWatermark(beamReader).getUninterruptibly();

        watermark = beamReaderDao.getWatermark(beamReader);
        Assert.assertTrue(watermark.isPresent());
        Assert.assertEquals(beamReader.getWaterMark(), watermark.get());
    }
}
