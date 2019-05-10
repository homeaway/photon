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
package com.homeaway.datatools.photon.utils.client;

import com.homeaway.datatools.photon.api.model.PhotonBeamReaderLockWrapper;
import com.homeaway.datatools.photon.client.JsonHelper;
import com.homeaway.datatools.photon.client.PhotonClientTestHelper;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.util.concurrent.locks.ReentrantLock;

public class PhotonBeamReaderLockWrapperTest {

    @Test
    public void testJsonRoundTrip() {
        PhotonBeamReader reader = PhotonClientTestHelper.buildPhotonBeamReader();
        reader.setWaterMark(Instant.now());
        PhotonBeamReaderLockWrapper wrapper = new PhotonBeamReaderLockWrapper();
        wrapper.setPhotonBeamReader(reader);
        wrapper.setLock(new ReentrantLock());
        Assert.assertEquals(wrapper.getPhotonBeamReader(), JsonHelper.jsonRoundTrip(wrapper, PhotonBeamReaderLockWrapper.class).getPhotonBeamReader());
    }

    @Test
    public void testPhotonBeamReaderLockWrapperOf() {
        PhotonBeamReader reader = PhotonClientTestHelper.buildPhotonBeamReader();
        reader.setWaterMark(Instant.now());
        PhotonBeamReaderLockWrapper wrapper = PhotonBeamReaderLockWrapper.of(reader);
        Assert.assertNotNull(wrapper.getLock());
        Assert.assertFalse(wrapper.getLock().isLocked());
        Assert.assertEquals(reader, wrapper.getPhotonBeamReader());
    }

    @Test
    public void testPhotonBeamReaderLockWrapperLock() {
        PhotonBeamReader reader = PhotonClientTestHelper.buildPhotonBeamReader();
        reader.setWaterMark(Instant.now());
        PhotonBeamReaderLockWrapper wrapper = PhotonBeamReaderLockWrapper.of(reader);
        Assert.assertFalse(wrapper.getLock().isLocked());
        wrapper.getLock().tryLock();
        Assert.assertTrue(wrapper.getLock().isLocked());
        wrapper.getLock().unlock();
        Assert.assertFalse(wrapper.getLock().isLocked());
    }
}
