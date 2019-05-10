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
package com.homeaway.datatools.photon.client.cache;

import com.homeaway.datatools.photon.api.model.PhotonBeamReaderLockWrapper;
import com.homeaway.datatools.photon.PhotonClientMockHelper;
import com.homeaway.datatools.photon.dao.beam.BeamReaderDao;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public class DefaultBeamReaderCacheTest {

    private static final UUID BEAM_UUID_1 = UUID.randomUUID();
    private static final UUID BEAM_UUID_2 = UUID.randomUUID();
    private static final UUID BEAM_UUID_3 = UUID.randomUUID();
    private static final UUID BEAM_UUID_4 = UUID.randomUUID();
    private static final UUID BEAM_UUID_5 = UUID.randomUUID();
    private static final UUID BEAM_UUID_6 = UUID.randomUUID();
    private static final String CLIENT_NAME_1 = "clientName1";
    private static final String CLIENT_NAME_2 = "clientName2";
    private static final String CLIENT_NAME_3 = "clientName3";
    private static final UUID BEAM_READER_UUID_1 = UUID.randomUUID();
    private static final UUID BEAM_READER_UUID_2 = UUID.randomUUID();
    private static final UUID BEAM_READER_UUID_3 = UUID.randomUUID();
    private static final UUID BEAM_READER_UUID_4 = UUID.randomUUID();
    private static final UUID BEAM_READER_UUID_5 = UUID.randomUUID();
    private static final UUID BEAM_READER_UUID_6 = UUID.randomUUID();
    private BeamReaderCache beamReaderCache;
    private BeamReaderDao beamReaderDao;

    @Before
    public void init() {
        beamReaderDao = PhotonClientMockHelper.mockBeamReaderDao();
        beamReaderCache = new DefaultBeamReaderCache(beamReaderDao);
        buildMap();
    }

    @Test
    public void testGetBeamReaderMissing() {
        PhotonBeam beam = new PhotonBeam();
        beam.setBeamUuid(UUID.randomUUID());
        beam.setStartDate(Instant.now());
        beam.setBeamName("Testing");
        Optional<PhotonBeamReaderLockWrapper> beamReader = beamReaderCache.getPhotonBeamReader("test", beam);
        Assert.assertFalse(beamReader.isPresent());
    }

    @Test
    public void testGetBeamReader() {
        PhotonBeam beam = new PhotonBeam();
        beam.setBeamUuid(BEAM_UUID_3);
        beam.setStartDate(Instant.now());
        beam.setBeamName("Testing");
        Optional<PhotonBeamReaderLockWrapper> beamReader = beamReaderCache.getPhotonBeamReader(CLIENT_NAME_2, beam);
        Assert.assertTrue(beamReader.isPresent());
    }

    @Test
    public void putBeamBeamReaderTest() {
        UUID beamUuid = UUID.randomUUID();
        String clientNameNew = "TestClientNameNew";
        PhotonBeam beam = new PhotonBeam();
        beam.setBeamUuid(beamUuid);
        beam.setBeamName("Test");
        beam.setStartDate(Instant.now());

        PhotonBeamReader photonBeamReader = new PhotonBeamReader();
        photonBeamReader.setClientName(clientNameNew);
        photonBeamReader.setWaterMark(Instant.now());
        photonBeamReader.setBeamReaderUuid(UUID.randomUUID());
        photonBeamReader.setBeamUuid(beamUuid);
        beamReaderCache.putPhotonBeamReader(beam, photonBeamReader);
        Optional<PhotonBeamReaderLockWrapper> test = beamReaderCache.getPhotonBeamReader(clientNameNew, beam);
        Assert.assertTrue(test.isPresent());
        Assert.assertEquals(photonBeamReader,test.get().getPhotonBeamReader());
    }

    private void buildMap() {
        beamReaderDao.putBeamReader(buildBeamReader(CLIENT_NAME_1, BEAM_UUID_1, BEAM_READER_UUID_1));
        beamReaderDao.putBeamReader(buildBeamReader(CLIENT_NAME_1, BEAM_UUID_2, BEAM_READER_UUID_2));
        beamReaderDao.putBeamReader(buildBeamReader(CLIENT_NAME_2, BEAM_UUID_3, BEAM_READER_UUID_3));
        beamReaderDao.putBeamReader(buildBeamReader(CLIENT_NAME_2, BEAM_UUID_4, BEAM_READER_UUID_4));
        beamReaderDao.putBeamReader(buildBeamReader(CLIENT_NAME_3, BEAM_UUID_5, BEAM_READER_UUID_5));
        beamReaderDao.putBeamReader(buildBeamReader(CLIENT_NAME_3, BEAM_UUID_6, BEAM_READER_UUID_6));
    }

    private PhotonBeamReader buildBeamReader(String clientName, UUID beamUuid, UUID beamReaderUuid) {
        PhotonBeamReader photonBeamReader = new PhotonBeamReader();
        photonBeamReader.setClientName(clientName);
        photonBeamReader.setBeamUuid(beamUuid);
        photonBeamReader.setBeamReaderUuid(beamReaderUuid);
        photonBeamReader.setWaterMark(Instant.now());
        return photonBeamReader;
    }
}
