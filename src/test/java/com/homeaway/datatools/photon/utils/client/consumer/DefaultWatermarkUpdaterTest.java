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
import com.homeaway.datatools.photon.client.PhotonClientTestHelper;
import com.homeaway.datatools.photon.client.consumer.partition.consumer.watermark.DefaultWatermarkUpdater;
import com.homeaway.datatools.photon.client.consumer.partition.consumer.watermark.WatermarkUpdater;
import com.homeaway.datatools.photon.dao.beam.BeamReaderDao;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;

public class DefaultWatermarkUpdaterTest {

    private BeamReaderDao beamReaderDao;

    @Test
    public void updateWaterMarkNoOverlap() {
        beamReaderDao = mockBeamReaderDao();
        WatermarkUpdater watermarkUpdater = new DefaultWatermarkUpdater(beamReaderDao);
        PhotonBeamReader photonBeamReader = PhotonClientTestHelper.buildPhotonBeamReader();
        photonBeamReader.setWaterMark(Instant.now());
        watermarkUpdater.updateWatermark(photonBeamReader);
        Assert.assertTrue(beamReaderDao.getWatermark(photonBeamReader).isPresent());
        Assert.assertEquals(photonBeamReader.getWaterMark(), beamReaderDao.getWatermark(photonBeamReader).get());
        photonBeamReader.setWaterMark(Instant.now().plusMillis(100));
        watermarkUpdater.updateWatermark(photonBeamReader);
        Assert.assertTrue(beamReaderDao.getWatermark(photonBeamReader).isPresent());
        Assert.assertEquals(photonBeamReader.getWaterMark(), beamReaderDao.getWatermark(photonBeamReader).get());

    }

    @Test
    public void updateWaterMarkWithOverlap() {
        beamReaderDao = mockBeamReaderDao(Duration.ofMillis(100));
        WatermarkUpdater watermarkUpdater = new DefaultWatermarkUpdater(beamReaderDao);
        PhotonBeamReader photonBeamReader = PhotonClientTestHelper.buildPhotonBeamReader();
        photonBeamReader.setWaterMark(Instant.now());
        Instant oldWatermark = photonBeamReader.getWaterMark();
        watermarkUpdater.updateWatermark(photonBeamReader);
        Assert.assertTrue(beamReaderDao.getWatermark(photonBeamReader).isPresent());
        Assert.assertEquals(photonBeamReader.getWaterMark(), beamReaderDao.getWatermark(photonBeamReader).get());
        photonBeamReader.setWaterMark(Instant.now().plusMillis(100));
        watermarkUpdater.updateWatermark(photonBeamReader);
        Assert.assertTrue(beamReaderDao.getWatermark(photonBeamReader).isPresent());
        Assert.assertNotEquals(photonBeamReader.getWaterMark(), beamReaderDao.getWatermark(photonBeamReader).get());
        Assert.assertEquals(oldWatermark, beamReaderDao.getWatermark(photonBeamReader).get());
    }
}
