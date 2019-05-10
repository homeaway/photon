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
package com.homeaway.datatools.photon.utils.client.consumer;

import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamReaderDao;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamReaderLockDao;
import static com.homeaway.datatools.photon.client.PhotonClientTestHelper.buildPhotonBeam;
import static com.homeaway.datatools.photon.client.PhotonClientTestHelper.buildPhotonBeamReader;
import com.homeaway.datatools.photon.client.cache.BeamReaderCache;
import com.homeaway.datatools.photon.client.cache.DefaultBeamReaderCache;
import com.homeaway.datatools.photon.dao.beam.BeamReaderDao;
import com.homeaway.datatools.photon.dao.beam.BeamReaderLockDao;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;

public class DefaultBeamReaderLockManagerTest {

    private static final Duration LOCK_THRESHOLD = Duration.ofSeconds(4);
    private BeamReaderCache cache;
    private BeamReaderCache cacheB;
    private BeamReaderLockDao beamReaderLockDao;
    private BeamReaderDao beamReaderDao;

    @Before
    public void init() {
        beamReaderDao = mockBeamReaderDao();
        cache = new DefaultBeamReaderCache(beamReaderDao);
        cacheB = new DefaultBeamReaderCache(beamReaderDao);
        beamReaderLockDao = mockBeamReaderLockDao(LOCK_THRESHOLD);
    }

    @Test
    public void startTest() throws Exception {
        PhotonBeam beam = buildPhotonBeam();
        PhotonBeamReader beamReader = buildPhotonBeamReader();
        beamReader.setBeamUuid(beam.getBeamUuid());
        cache.putPhotonBeamReader(beam, beamReader);
        DefaultBeamReaderLockManager lockManager = new DefaultBeamReaderLockManager(Executors.newScheduledThreadPool(5),
                Executors.newFixedThreadPool(50), cache,
                beamReaderLockDao, LOCK_THRESHOLD);
        lockManager.start();
        Assert.assertFalse(beamReader.getPhotonBeamReaderLock().isPresent());
        try {
            Thread.sleep(LOCK_THRESHOLD.toMillis());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Assert.assertTrue(beamReader.getPhotonBeamReaderLock().isPresent());
    }

    @Test
    public void startMultipleThreads() throws Exception {
        PhotonBeam beam = buildPhotonBeam();
        PhotonBeamReader beamReaderA = buildPhotonBeamReader();
        PhotonBeamReader beamReaderB = buildPhotonBeamReader();
        beamReaderA.setBeamUuid(beam.getBeamUuid());
        beamReaderB.setBeamUuid(beam.getBeamUuid());
        cache.putPhotonBeamReader(beam, beamReaderA);
        cacheB.putPhotonBeamReader(beam, beamReaderB);
        DefaultBeamReaderLockManager lockManagerA = new DefaultBeamReaderLockManager(Executors.newScheduledThreadPool(5),
                Executors.newFixedThreadPool(50), cache,
                beamReaderLockDao, LOCK_THRESHOLD);
        DefaultBeamReaderLockManager lockManagerB = new DefaultBeamReaderLockManager(Executors.newScheduledThreadPool(5),
                Executors.newFixedThreadPool(50), cacheB,
                beamReaderLockDao, LOCK_THRESHOLD);
        lockManagerA.start();
        lockManagerB.start();
        Assert.assertFalse(beamReaderA.getPhotonBeamReaderLock().isPresent());
        Assert.assertFalse(beamReaderB.getPhotonBeamReaderLock().isPresent());
        try {
            Thread.sleep(LOCK_THRESHOLD.toMillis());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Assert.assertTrue(beamReaderA.getPhotonBeamReaderLock().isPresent() || beamReaderB.getPhotonBeamReaderLock().isPresent());
        Assert.assertTrue(!beamReaderA.getPhotonBeamReaderLock().isPresent() || !beamReaderB.getPhotonBeamReaderLock().isPresent());
        if (beamReaderA.getPhotonBeamReaderLock().isPresent()) {
            lockManagerA.stop();
            try {
                Thread.sleep(LOCK_THRESHOLD.multipliedBy(3).toMillis());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            Assert.assertTrue(beamReaderB.getPhotonBeamReaderLock().isPresent());
        } else {
            lockManagerB.stop();
            try {
                Thread.sleep(LOCK_THRESHOLD.multipliedBy(3).toMillis());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            Assert.assertTrue(beamReaderA.getPhotonBeamReaderLock().isPresent());
        }
    }

    @Test
    public void testUpdateCycle() throws Exception {
        PhotonBeam beam = buildPhotonBeam();
        PhotonBeamReader beamReader = buildPhotonBeamReader();
        beamReader.setBeamUuid(beam.getBeamUuid());
        cache.putPhotonBeamReader(beam, beamReader);
        DefaultBeamReaderLockManager lockManager = new DefaultBeamReaderLockManager(Executors.newScheduledThreadPool(5),
                Executors.newFixedThreadPool(50), cache,
                beamReaderLockDao, LOCK_THRESHOLD);
        lockManager.start();
        Assert.assertFalse(beamReader.getPhotonBeamReaderLock().isPresent());
        try {
            Thread.sleep(LOCK_THRESHOLD.toMillis());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Assert.assertTrue(beamReader.getPhotonBeamReaderLock().isPresent());
        Instant currentLockTime = beamReader.getPhotonBeamReaderLock().get().getLockTime();
        try {
            Thread.sleep(LOCK_THRESHOLD.toMillis());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Assert.assertTrue(currentLockTime.isBefore(beamReader.getPhotonBeamReaderLock().get().getLockTime()));
        Assert.assertEquals(beamReader.getPhotonBeamReaderLock().get(), beamReaderLockDao.getPhotonBeamLock(beamReader));
    }

}
