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
package com.homeaway.datatools.photon.client.consumer;

import com.google.common.collect.Lists;
import com.homeaway.datatools.photon.api.beam.PhotonMessageHandler;
import com.homeaway.datatools.photon.api.model.PhotonBeamReaderLockWrapper;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamDao;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamReaderDao;
import com.homeaway.datatools.photon.client.cache.BeamCache;
import com.homeaway.datatools.photon.client.cache.BeamReaderCache;
import com.homeaway.datatools.photon.client.cache.DefaultBeamCache;
import com.homeaway.datatools.photon.client.cache.DefaultBeamReaderCache;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import static com.homeaway.datatools.photon.api.model.enums.PhotonBeamReaderOffsetType.FROM_BEGINNING;
import static com.homeaway.datatools.photon.api.model.enums.PhotonBeamReaderOffsetType.FROM_CURRENT;
import com.homeaway.datatools.photon.utils.client.consumer.BeamReaderConfigManager;
import com.homeaway.datatools.photon.utils.client.consumer.DefaultBeamReaderConfigManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.mock;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class DefaultBeamReaderConfigManagerTest {

    private BeamCache beamCache;
    private BeamReaderCache beamReaderCache;
    private BeamReaderConfigManager beamReaderConfigManager;

    @Before
    public void init() {
        beamCache = new DefaultBeamCache(mockBeamDao());
        beamReaderCache = new DefaultBeamReaderCache(mockBeamReaderDao());
        beamReaderConfigManager = new DefaultBeamReaderConfigManager(beamCache, beamReaderCache);
    }

    @Test
    public void testPutBeamReaderConfigNoBeamNoNewReader() {
        String CLIENT_NAME = "TestClientName";
        String BEAM_NAME = "TestBeamName";

        PhotonMessageHandler handler = mock(PhotonMessageHandler.class);

        PhotonBeamReaderConfig config = new PhotonBeamReaderConfig(CLIENT_NAME, BEAM_NAME,
                handler, FROM_CURRENT, (clientName, beamName) -> Instant.now());

        beamReaderConfigManager.putBeamReaderConfig(config);
        Optional<PhotonBeamReaderConfig> beamReaderConfig = beamReaderConfigManager.getBeamReaderConfig(CLIENT_NAME, BEAM_NAME);
        Assert.assertTrue(beamReaderConfig.isPresent());
        Assert.assertEquals(handler, beamReaderConfig.get().getPhotonMessageHandler());
        Assert.assertEquals(FROM_CURRENT, beamReaderConfig.get().getOffsetType());
        Assert.assertEquals(0, beamReaderCache.getCacheAsMap().size());
    }

    @Test
    public void testPutBeamReaderConfigWithBeamNoNewReader() {
        String CLIENT_NAME = "TestClientName";
        String BEAM_NAME = "TestBeamName";

        PhotonBeam beam = new PhotonBeam();
        beam.setBeamUuid(UUID.randomUUID());
        beam.setStartDate(Instant.now().minus(10L, ChronoUnit.HOURS));
        beam.setBeamName(BEAM_NAME);
        beamCache.putBeam(beam);

        Assert.assertTrue(beamCache.getBeamByUuid(beam.getBeamUuid()).isPresent());
        Assert.assertFalse(beamCache.getBeamByName(beam.getBeamName()).isEmpty());

        PhotonMessageHandler handler = mock(PhotonMessageHandler.class);

        PhotonBeamReaderConfig config = new PhotonBeamReaderConfig(CLIENT_NAME, BEAM_NAME,
                handler, FROM_CURRENT, (clientName, beamName) -> Instant.now());

        beamReaderConfigManager.putBeamReaderConfig(config);
        Optional<PhotonBeamReaderConfig> beamReaderConfig = beamReaderConfigManager.getBeamReaderConfig(CLIENT_NAME, BEAM_NAME);
        Assert.assertTrue(beamReaderConfig.isPresent());
        Assert.assertEquals(handler, beamReaderConfig.get().getPhotonMessageHandler());
        Assert.assertEquals(FROM_CURRENT, beamReaderConfig.get().getOffsetType());
        Assert.assertEquals(0, beamReaderCache.getCacheAsMap().size());
    }

    @Test
    public void testPutBeamReaderConfigNoBeamNewReader() {
        String CLIENT_NAME = "TestClientNameFromBeginning";
        String BEAM_NAME = "TestBeamNameFromBeginning";

        PhotonMessageHandler handler = mock(PhotonMessageHandler.class);

        PhotonBeamReaderConfig config = new PhotonBeamReaderConfig(CLIENT_NAME, BEAM_NAME,
                handler, FROM_BEGINNING, (clientName, beamName) -> Instant.now());

        beamReaderConfigManager.putBeamReaderConfig(config);
        Optional<PhotonBeamReaderConfig> beamReaderConfig = beamReaderConfigManager.getBeamReaderConfig(CLIENT_NAME, BEAM_NAME);
        Assert.assertTrue(beamReaderConfig.isPresent());
        Assert.assertEquals(handler, beamReaderConfig.get().getPhotonMessageHandler());
        Assert.assertEquals(FROM_BEGINNING, beamReaderConfig.get().getOffsetType());
        Assert.assertEquals(0, beamReaderCache.getCacheAsMap().size());
    }

    @Test
    public void testPutBeamReaderConfigWithBeamNewReader() {
        String CLIENT_NAME = "TestClientNameFromBeginning";
        String BEAM_NAME = "TestBeamNameFromBeginning";

        PhotonBeam beam = new PhotonBeam();
        beam.setBeamUuid(UUID.randomUUID());
        beam.setStartDate(Instant.now().minus(10L, ChronoUnit.HOURS));
        beam.setBeamName(BEAM_NAME);
        beamCache.putBeam(beam);

        Assert.assertTrue(beamCache.getBeamByUuid(beam.getBeamUuid()).isPresent());
        Assert.assertFalse(beamCache.getBeamByName(beam.getBeamName()).isEmpty());

        PhotonMessageHandler handler = mock(PhotonMessageHandler.class);

        PhotonBeamReaderConfig config = new PhotonBeamReaderConfig(CLIENT_NAME, BEAM_NAME,
                handler, FROM_BEGINNING, (clientName, beamName) -> Instant.now());

        beamReaderConfigManager.putBeamReaderConfig(config);
        Optional<PhotonBeamReaderConfig> beamReaderConfig = beamReaderConfigManager.getBeamReaderConfig(CLIENT_NAME, BEAM_NAME);
        Optional<PhotonBeamReaderLockWrapper> beamReaderLockWrapper = beamReaderCache.getPhotonBeamReader(CLIENT_NAME, beam);
        Assert.assertTrue(beamReaderConfig.isPresent());
        Assert.assertEquals(handler, beamReaderConfig.get().getPhotonMessageHandler());
        Assert.assertEquals(FROM_BEGINNING, beamReaderConfig.get().getOffsetType());
        Assert.assertEquals(1, beamReaderCache.getCacheAsMap().size());
        Assert.assertTrue(beamReaderLockWrapper.isPresent());

        beamReaderConfigManager.putBeamReaderConfig(config);
        Assert.assertEquals(1, beamReaderCache.getCacheAsMap().size());
        Assert.assertTrue(beamReaderCache.getPhotonBeamReader(CLIENT_NAME, beam).isPresent());
        Assert.assertNotEquals(beamReaderLockWrapper.get().getPhotonBeamReader().getBeamReaderUuid(),
                beamReaderCache.getPhotonBeamReader(CLIENT_NAME, beam).get().getPhotonBeamReader().getBeamReaderUuid());

    }

    @Test
    public void testPutBeamReaderConfigCreateNewBeamNewReader() {
        String CLIENT_NAME = "TestClientNameFromBeginning";
        String BEAM_NAME = "TestBeamNameFromBeginning";

        PhotonBeam beam = new PhotonBeam();
        beam.setBeamUuid(UUID.randomUUID());
        beam.setStartDate(Instant.now().minus(10L, ChronoUnit.HOURS));
        beam.setBeamName(BEAM_NAME);
        beamCache.putBeam(beam);

        Assert.assertTrue(beamCache.getBeamByUuid(beam.getBeamUuid()).isPresent());
        Assert.assertFalse(beamCache.getBeamByName(beam.getBeamName()).isEmpty());

        PhotonMessageHandler handler = mock(PhotonMessageHandler.class);

        PhotonBeamReaderConfig config = new PhotonBeamReaderConfig(CLIENT_NAME, BEAM_NAME,
                handler, FROM_CURRENT, (clientName, beamName) -> Instant.now());

        Assert.assertEquals(0, beamReaderCache.getCacheAsMap().size());
        beamReaderConfigManager.createNewReader(config);

        Assert.assertEquals(1, beamReaderCache.getCacheAsMap().size());
        Assert.assertTrue(beamReaderCache.getPhotonBeamReader(CLIENT_NAME, beam).isPresent());
    }

    @Test
    public void testIterateConfigsSameBeamDifferentClient() {
        List<PhotonBeamReaderConfig> configs = Lists.newArrayList();
        String CLIENT_NAME_TEMPLATE = "TestClientNameFromBeginning_%s";
        String BEAM_NAME = "TestBeamNameFromBeginning";
        PhotonMessageHandler handler = mock(PhotonMessageHandler.class);

        for (int i = 0; i < 10; i++) {
            PhotonBeamReaderConfig config = new PhotonBeamReaderConfig(String.format(CLIENT_NAME_TEMPLATE, i),
                    BEAM_NAME, handler, FROM_CURRENT, (clientName, beamName) -> Instant.now());
            beamReaderConfigManager.putBeamReaderConfig(config);
        }

        beamReaderConfigManager.iterateConfigs(configs::add);

        Assert.assertEquals(10, configs.size());
    }

    @Test
    public void testIterateConfigsSameClientDifferentBeam() {
        List<PhotonBeamReaderConfig> configs = Lists.newArrayList();
        String CLIENT_NAME = "TestClientNameFromBeginning";
        String BEAM_NAME_TEMPLATE = "TestBeamNameFromBeginning_%s";
        PhotonMessageHandler handler = mock(PhotonMessageHandler.class);

        for (int i = 0; i < 10; i++) {
            PhotonBeamReaderConfig config = new PhotonBeamReaderConfig(CLIENT_NAME,
                    String.format(BEAM_NAME_TEMPLATE, i), handler, FROM_CURRENT, (clientName, beamName) -> Instant.now());
            beamReaderConfigManager.putBeamReaderConfig(config);
        }

        beamReaderConfigManager.iterateConfigs(configs::add);

        Assert.assertEquals(10, configs.size());
    }

    @Test
    public void testIterateConfigsDifferentClientDifferentBeam() {
        List<PhotonBeamReaderConfig> configs = Lists.newArrayList();
        String CLIENT_NAME_TEMPLATE = "TestClientNameFromBeginning_%s";
        String BEAM_NAME_TEMPLATE = "TestBeamNameFromBeginning_%s";
        PhotonMessageHandler handler = mock(PhotonMessageHandler.class);

        for (int i = 0; i < 10; i++) {
            PhotonBeamReaderConfig config = new PhotonBeamReaderConfig(String.format(CLIENT_NAME_TEMPLATE, i),
                    String.format(BEAM_NAME_TEMPLATE, i), handler, FROM_CURRENT, (clientName, beamName) -> Instant.now());
            beamReaderConfigManager.putBeamReaderConfig(config);
        }

        beamReaderConfigManager.iterateConfigs(configs::add);

        Assert.assertEquals(10, configs.size());
    }

    @Test
    public void testIterateConfigsSameClientSameBeam() {
        List<PhotonBeamReaderConfig> configs = Lists.newArrayList();
        String CLIENT_NAME = "TestClientNameFromBeginning";
        String BEAM_NAME = "TestBeamNameFromBeginning";
        PhotonMessageHandler handler = mock(PhotonMessageHandler.class);

        for (int i = 0; i < 10; i++) {
            PhotonBeamReaderConfig config = new PhotonBeamReaderConfig(CLIENT_NAME,
                    BEAM_NAME, handler, FROM_CURRENT, (clientName, beamName) -> Instant.now());
            beamReaderConfigManager.putBeamReaderConfig(config);
        }

        beamReaderConfigManager.iterateConfigs(configs::add);

        Assert.assertEquals(1, configs.size());
    }

    @Test
    public void testIterateConfigsEmpty() {
        List<PhotonBeamReaderConfig> configs = Lists.newArrayList();

        beamReaderConfigManager.iterateConfigs(configs::add);

        Assert.assertEquals(0, configs.size());
    }
}
