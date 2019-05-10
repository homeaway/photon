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
import com.homeaway.datatools.photon.dao.beam.cassandra.CassandraBeamReaderLockDao;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReaderLock;
import com.homeaway.datatools.photon.utils.test.cassandra.testing.EmbeddedCassandraRule;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Slf4j
public class CassandraBeamReaderLockDaoTest {

    private static final Duration LOCK_THRESHOLD = Duration.ofSeconds(5);
    private BeamReaderLockDao beamReaderLockDao;
    private Session session;

    @Rule
    public EmbeddedCassandraRule embeddedCassandraRule = new EmbeddedCassandraRule("cassandra/create_keyspace.cql",
            "cassandra/beam_reader.cql");


    @Before
    public void init() {
        session = embeddedCassandraRule.cluster().connect("photon");
        this.beamReaderLockDao = new CassandraBeamReaderLockDao(session, LOCK_THRESHOLD);
    }

    @Test
    public void putBeamReaderLockTest() {
        String clientName = "TestService";
        UUID beamUuid = UUID.randomUUID();
        PhotonBeamReaderLock lock = new PhotonBeamReaderLock(UUID.randomUUID(), Instant.ofEpochMilli(Instant.now().toEpochMilli()));
        PhotonBeamReaderLock testLock = beamReaderLockDao.putBeamReaderLock(clientName, beamUuid, lock);
        Assert.assertEquals(lock, testLock);
    }

    @Test
    public void updateLockTest() {
        String clientName = "TestService";
        UUID beamUuid = UUID.randomUUID();
        PhotonBeamReader beamReader = new PhotonBeamReader();
        beamReader.setBeamUuid(beamUuid);
        beamReader.setClientName(clientName);
        Instant now = Instant.ofEpochMilli(Instant.now().toEpochMilli());
        beamReader.setPhotonBeamReaderLock(new PhotonBeamReaderLock(UUID.randomUUID(), now));
        PhotonBeamReaderLock lock = beamReaderLockDao.putBeamReaderLock(clientName, beamUuid, beamReader.getPhotonBeamReaderLock().get());
        Assert.assertEquals(beamReader.getPhotonBeamReaderLock().get(), lock);
        Assert.assertEquals(now, lock.getLockTime());

        now = Instant.ofEpochMilli(Instant.now().toEpochMilli());
        beamReader.getPhotonBeamReaderLock().get().setLockTime(now);
        beamReaderLockDao.updateLock(beamReader);
        lock = beamReaderLockDao.getPhotonBeamLock(beamReader);
        Assert.assertNotNull(lock);
        Assert.assertEquals(beamReader.getPhotonBeamReaderLock().get(), lock);
        Assert.assertEquals(now, lock.getLockTime());
    }

    @Test
    public void getAvailablePhotonBeamLockTest() {
        String clientName = "TestService";
        UUID beamUuid = UUID.randomUUID();
        PhotonBeamReader beamReader = new PhotonBeamReader();
        beamReader.setBeamUuid(beamUuid);
        beamReader.setClientName(clientName);
        Instant now = Instant.ofEpochMilli(Instant.now().toEpochMilli());
        beamReader.setPhotonBeamReaderLock(new PhotonBeamReaderLock(UUID.randomUUID(), now));
        PhotonBeamReaderLock lock = beamReaderLockDao.putBeamReaderLock(clientName, beamUuid, beamReader.getPhotonBeamReaderLock().get());
        Assert.assertEquals(beamReader.getPhotonBeamReaderLock().get(), lock);
        Assert.assertEquals(now, lock.getLockTime());
        Optional<PhotonBeamReaderLock> availableLock = beamReaderLockDao.getAvailablePhotonBeamLock(beamReader);
        Assert.assertFalse(availableLock.isPresent());
        try {
            Thread.sleep(LOCK_THRESHOLD.toMillis());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        availableLock = beamReaderLockDao.getAvailablePhotonBeamLock(beamReader);
        Assert.assertTrue(availableLock.isPresent());
    }

    @Test
    public void getAvailablePhotonBeamLockTestNulls() {
        BeamReaderDao beamReaderDao = new CassandraBeamReaderDao(session);
        String clientName = "TestService";
        UUID beamUuid = UUID.randomUUID();
        PhotonBeamReader beamReader = new PhotonBeamReader();
        beamReader.setBeamUuid(beamUuid);
        beamReader.setClientName(clientName);
        beamReader.setWaterMark(Instant.now());
        beamReaderDao.putBeamReader(beamReader);
        Optional<PhotonBeamReaderLock> availableLock = beamReaderLockDao.getAvailablePhotonBeamLock(beamReader);
        Assert.assertTrue(availableLock.isPresent());
    }

    @Test
    public void getPhotonBeamLockTest() {
        String clientName = "TestService";
        UUID beamUuid = UUID.randomUUID();
        PhotonBeamReader beamReader = new PhotonBeamReader();
        beamReader.setBeamUuid(beamUuid);
        beamReader.setClientName(clientName);
        Instant now = Instant.ofEpochMilli(Instant.now().toEpochMilli());
        beamReader.setPhotonBeamReaderLock(new PhotonBeamReaderLock(UUID.randomUUID(), now));
        PhotonBeamReaderLock lock = beamReaderLockDao.putBeamReaderLock(clientName, beamUuid, beamReader.getPhotonBeamReaderLock().get());
        Assert.assertEquals(beamReader.getPhotonBeamReaderLock().get(), lock);
        Assert.assertEquals(now, lock.getLockTime());
        PhotonBeamReaderLock newLock = beamReaderLockDao.getPhotonBeamLock(beamReader);
        Assert.assertNotNull(newLock);
        Assert.assertEquals(lock, newLock);
    }


}
