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
package com.homeaway.datatools.photon.utils.dao.mapper;

import com.google.common.collect.Maps;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockPhotonRow;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReaderLock;
import com.homeaway.datatools.photon.dao.rows.PhotonRow;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_DEFAULT_TTL_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_NAME_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_READER_LOCK_TIME_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_READER_LOCK_UUID_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_READER_UUID_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_UUID_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.CLIENT_NAME_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.START_DATE_COLUMN;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public class DefaultEntityMapperTest {

    private EntityMapper entityMapper;

    @Before
    public void init() {
        entityMapper = new DefaultEntityMapper();
    }

    @Test(expected = ClassNotFoundException.class)
    public void testGetMappedBadClass() throws ClassNotFoundException {
        PhotonRow row = mock(PhotonRow.class);
        UUID test = entityMapper.getMappedEntity(row, UUID.class);
    }

    @Test
    public void testGetMappedEntityPhotonBeam() throws ClassNotFoundException {
        String beamName = "TestBeam";
        UUID beamUuid = UUID.randomUUID();
        Instant startDate = Instant.now();
        Map<String, Object> values = Maps.newHashMap();
        values.put(BEAM_NAME_COLUMN, beamName);
        values.put(BEAM_UUID_COLUMN, beamUuid);
        values.put(START_DATE_COLUMN, startDate);
        values.put(BEAM_DEFAULT_TTL_COLUMN, 100);
        PhotonBeam photonBeam = entityMapper.getMappedEntity(mockPhotonRow(values), PhotonBeam.class);
        Assert.assertEquals(beamUuid, photonBeam.getBeamUuid());
        Assert.assertEquals(startDate, photonBeam.getStartDate());
        Assert.assertEquals(beamName, photonBeam.getBeamName());
        Assert.assertEquals(100L, (long)photonBeam.getDefaultTtl());
    }

    @Test(expected = RuntimeException.class)
    public void testGetMappedEntityPhotonBeamNoTtlFailure() throws ClassNotFoundException {
        PhotonRow row = mock(PhotonRow.class);
        doThrow(RuntimeException.class).when(row).getInt(BEAM_DEFAULT_TTL_COLUMN);
        PhotonBeam photonBeam = entityMapper.getMappedEntity(row, PhotonBeam.class);
    }

    @Test
    public void testGetMappedEntityPhotonBeamReader() throws ClassNotFoundException {
        String clientName = "TestService";
        UUID beamUuid = UUID.randomUUID();
        UUID beamReaderUuid = UUID.randomUUID();
        Map<String, Object> values = Maps.newHashMap();
        values.put(CLIENT_NAME_COLUMN, clientName);
        values.put(BEAM_UUID_COLUMN, beamUuid);
        values.put(BEAM_READER_UUID_COLUMN, beamReaderUuid);
        PhotonBeamReader photonBeamReader = entityMapper.getMappedEntity(mockPhotonRow(values), PhotonBeamReader.class);
        Assert.assertEquals(clientName, photonBeamReader.getClientName());
        Assert.assertEquals(beamUuid, photonBeamReader.getBeamUuid());
        Assert.assertEquals(beamReaderUuid, photonBeamReader.getBeamReaderUuid());
        Assert.assertNull(photonBeamReader.getWaterMark());
        Assert.assertFalse(photonBeamReader.getPhotonBeamReaderLock().isPresent());
    }

    @Test
    public void testGetMappedBeamReaderLock() {
        Instant now = Instant.now();
        UUID lockUuid = UUID.randomUUID();
        Map<String, Object> values = Maps.newHashMap();
        values.put(BEAM_READER_LOCK_UUID_COLUMN, lockUuid);
        values.put(BEAM_READER_LOCK_TIME_COLUMN, now);
        try {
            PhotonBeamReaderLock lock = entityMapper.getMappedEntity(mockPhotonRow(values), PhotonBeamReaderLock.class);
            Assert.assertEquals(now, lock.getLockTime());
            Assert.assertEquals(lockUuid, lock.getLockUuid());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testGetMappedBeamReaderLockNulls() {
        try {
            PhotonBeamReaderLock lock = entityMapper.getMappedEntity(mockPhotonRow(Maps.newHashMap()), PhotonBeamReaderLock.class);
            Assert.assertNull(lock.getLockTime());
            Assert.assertNull(lock.getLockUuid());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
