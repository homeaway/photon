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

import com.google.common.collect.Maps;
import com.homeaway.datatools.photon.PhotonClientMockHelper;
import com.homeaway.datatools.photon.dao.beam.BeamDao;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

public class DefaultBeamCacheTest {

    private static final UUID BEAM_UUID_1 = UUID.randomUUID();
    private static final UUID BEAM_UUID_2 = UUID.randomUUID();
    private static final UUID BEAM_UUID_3 = UUID.randomUUID();
    private static final UUID BEAM_UUID_4 = UUID.randomUUID();
    private static final String BEAM_NAME_1 = "TestBeam1";
    private static final String BEAM_NAME_2 = "TestBeam2";
    private static final String BEAM_NAME_3 = "TestBeam3";
    private static final String BEAM_NAME_4 = "TestBeam4";

    private ConcurrentMap<String, Map<UUID, PhotonBeam>> beamsByName;
    private ConcurrentMap<UUID, PhotonBeam> beamsByUuid;
    private BeamDao beamDao;
    private BeamCache beamCache;

    @Before
    public void init() {
        beamsByName = Maps.newConcurrentMap();
        beamsByUuid = Maps.newConcurrentMap();
        buildMaps();
        beamDao = PhotonClientMockHelper.mockBeamDao(beamsByName, beamsByUuid);
        beamCache = new DefaultBeamCache(beamDao);
    }

    @Test
    public void testGetBeamByNameMissing() {
        Queue<PhotonBeam> beams = beamCache.getBeamByName("Test");
        Assert.assertTrue(beams.isEmpty());
        Assert.assertNull(beams.peek());
    }

    @Test
    public void testGetBeamByName() {
        Queue<PhotonBeam> beams = beamCache.getBeamByName(BEAM_NAME_3);
        Assert.assertFalse(beams.isEmpty());
        Assert.assertEquals(BEAM_UUID_3, beams.peek().getBeamUuid());
    }

    @Test
    public void testGetBeamByUuidMissing() {
        Optional<PhotonBeam> beam = beamCache.getBeamByUuid(UUID.randomUUID());
        Assert.assertFalse(beam.isPresent());
    }

    @Test
    public void testGetBeamByUuid() {
        Optional<PhotonBeam> beam = beamCache.getBeamByUuid(BEAM_UUID_2);
        Assert.assertTrue(beam.isPresent());
        Assert.assertEquals(BEAM_NAME_2, beam.get().getBeamName());
    }

    @Test
    public void putBeamTest() {
        UUID newBeamUuid = UUID.randomUUID();
        String newBeamName = "TestNewBeam";
        PhotonBeam beam = new PhotonBeam();
        beam.setBeamUuid(newBeamUuid);
        beam.setBeamName(newBeamName);
        beam.setStartDate(Instant.now());
        beamCache.putBeam(beam);
        Queue<PhotonBeam> beamByName = beamCache.getBeamByName(newBeamName);
        Optional<PhotonBeam> beamByUuid = beamCache.getBeamByUuid(newBeamUuid);
        Assert.assertFalse(beamByName.isEmpty());
        Assert.assertTrue(beamByUuid.isPresent());
        Assert.assertEquals(beamByName.peek(), beamByUuid.get());
    }

    @Test
    public void testExpiration() {
        Queue<PhotonBeam> beams = beamCache.getBeamByName(BEAM_NAME_3);
        Assert.assertFalse(beams.isEmpty());
        Assert.assertEquals(BEAM_UUID_3, beams.peek().getBeamUuid());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        beams = beamCache.getBeamByNameIfPresent(BEAM_NAME_3);
        Assert.assertTrue(beams.isEmpty());
    }

    private void buildMaps() {
        beamsByUuid.put(BEAM_UUID_1, buildBeam(BEAM_UUID_1, BEAM_NAME_1));
        beamsByUuid.put(BEAM_UUID_2, buildBeam(BEAM_UUID_2, BEAM_NAME_2));
        beamsByUuid.put(BEAM_UUID_3, buildBeam(BEAM_UUID_3, BEAM_NAME_3));
        beamsByUuid.put(BEAM_UUID_4, buildBeam(BEAM_UUID_4, BEAM_NAME_4));

        Map<UUID, PhotonBeam> beamMap = Maps.newTreeMap();
        beamMap.put(BEAM_UUID_1, buildBeam(BEAM_UUID_1, BEAM_NAME_1));
        beamsByName.put(BEAM_NAME_1, beamMap);

        beamMap = Maps.newTreeMap();
        beamMap.put(BEAM_UUID_2, buildBeam(BEAM_UUID_2, BEAM_NAME_2));
        beamsByName.put(BEAM_NAME_2, beamMap);

        beamMap = Maps.newTreeMap();
        beamMap.put(BEAM_UUID_3, buildBeam(BEAM_UUID_3, BEAM_NAME_3));
        beamsByName.put(BEAM_NAME_3, beamMap);

        beamMap = Maps.newTreeMap();
        beamMap.put(BEAM_UUID_4, buildBeam(BEAM_UUID_4, BEAM_NAME_4));
        beamsByName.put(BEAM_NAME_4, beamMap);
    }

    private PhotonBeam buildBeam(UUID beamUuid, String beamName) {
        PhotonBeam beam = new PhotonBeam();
        beam.setBeamUuid(beamUuid);
        beam.setBeamName(beamName);
        beam.setStartDate(Instant.now());
        return beam;
    }
}
