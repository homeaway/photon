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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.Executors;

public class LockGetSchedulerTest {

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

        PhotonBeam beam = buildPhotonBeam();
        PhotonBeamReader beamReader = buildPhotonBeamReader();
        beamReader.setBeamUuid(beam.getBeamUuid());
        cache.putPhotonBeamReader(beam, beamReader);
        PhotonScheduler lockGetScheduler = new LockGetScheduler(Executors.newFixedThreadPool(5),
                cache, beamReaderLockDao, Executors.newScheduledThreadPool(5), LOCK_THRESHOLD);
        lockGetScheduler.start();
        Assert.assertFalse(beamReader.getPhotonBeamReaderLock().isPresent());
        try {
            Thread.sleep(LOCK_THRESHOLD.toMillis());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Assert.assertTrue(beamReader.getPhotonBeamReaderLock().isPresent());
    }
}
