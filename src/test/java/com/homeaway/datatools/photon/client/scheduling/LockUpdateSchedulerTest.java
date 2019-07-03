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
package com.homeaway.datatools.photon.client.scheduling;

import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamReaderDao;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamReaderLockDao;
import static com.homeaway.datatools.photon.client.PhotonClientTestHelper.buildPhotonBeam;
import static com.homeaway.datatools.photon.client.PhotonClientTestHelper.buildPhotonBeamReader;
import com.homeaway.datatools.photon.client.cache.BeamReaderCache;
import com.homeaway.datatools.photon.client.cache.DefaultBeamReaderCache;
import com.homeaway.datatools.photon.dao.beam.BeamReaderLockDao;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReaderLock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

public class LockUpdateSchedulerTest {

    private static final Duration LOCK_THRESHOLD = Duration.ofSeconds(4);
    private BeamReaderCache cache;
    private BeamReaderLockDao beamReaderLockDao;

    @Before
    public void init() {
        cache = new DefaultBeamReaderCache(mockBeamReaderDao());
        beamReaderLockDao = mockBeamReaderLockDao(LOCK_THRESHOLD);
    }

    @Test
    public void startTest() throws Exception {
        Instant start = Instant.now();
        PhotonBeam beam = buildPhotonBeam();
        PhotonBeamReader beamReader = buildPhotonBeamReader();
        beamReader.setBeamUuid(beam.getBeamUuid());
        beamReader.setPhotonBeamReaderLock(new PhotonBeamReaderLock(UUID.randomUUID(), start));
        cache.putPhotonBeamReader(beam, beamReader);
        PhotonScheduler lockUpdateScheduler = new LockUpdateScheduler(cache, beamReaderLockDao, LOCK_THRESHOLD);
        lockUpdateScheduler.start();
        try {
            Thread.sleep(LOCK_THRESHOLD.dividedBy(2).plusMillis(500).toMillis());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Assert.assertTrue(beamReader.getPhotonBeamReaderLock().isPresent());
        Assert.assertTrue(beamReader.getPhotonBeamReaderLock().get().getLockTime().isAfter(start));
    }

    @Test
    public void testOldLock() throws Exception {
        PhotonBeam beam = buildPhotonBeam();
        PhotonBeamReader beamReader = buildPhotonBeamReader();
        beamReader.setBeamUuid(beam.getBeamUuid());
        beamReader.setPhotonBeamReaderLock(new PhotonBeamReaderLock(UUID.randomUUID(), Instant.now().minus(LOCK_THRESHOLD).minusMillis(100)));
        cache.putPhotonBeamReader(beam, beamReader);
        PhotonScheduler lockUpdateScheduler = new LockUpdateScheduler(cache, beamReaderLockDao, LOCK_THRESHOLD);
        lockUpdateScheduler.start();
        try {
            Thread.sleep(LOCK_THRESHOLD.dividedBy(2).toMillis());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Assert.assertFalse(beamReader.getPhotonBeamReaderLock().isPresent());
    }
}
