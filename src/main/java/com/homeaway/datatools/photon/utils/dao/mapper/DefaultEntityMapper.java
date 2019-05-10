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

import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import com.homeaway.datatools.photon.dao.rows.PhotonRow;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_DEFAULT_TTL_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_NAME_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_READER_LOCK_TIME_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_READER_LOCK_UUID_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_READER_UUID_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_UUID_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.CLIENT_NAME_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.SCHEMA_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.START_DATE_COLUMN;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReaderLock;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;

@Slf4j
public class DefaultEntityMapper implements EntityMapper {

    @Override
    public <T> T getMappedEntity(PhotonRow row, Class<T> clazz) throws ClassNotFoundException {
        if(clazz.equals(PhotonBeam.class)) {
            return clazz.cast(getMappedBeam(row));
        } else if (clazz.equals(PhotonBeamReader.class)) {
            return clazz.cast(getMappedBeamReader(row));
        } else if (clazz.equals(PhotonBeamReaderLock.class)) {
            return clazz.cast(getMappedBeamReaderLock(row));
        } else if (clazz.equals(Schema.class)) {
            return clazz.cast(getMappedSchema(row));
        } else {
            throw new ClassNotFoundException(String.format("Could not map class %s", clazz.getName()));
        }
    }

    private static PhotonBeam getMappedBeam(PhotonRow row) throws ClassNotFoundException {
        PhotonBeam photonBeam = new PhotonBeam();
        photonBeam.setBeamName(row.getString(BEAM_NAME_COLUMN));
        photonBeam.setBeamUuid(row.getUuid(BEAM_UUID_COLUMN));
        photonBeam.setStartDate(row.getInstant(START_DATE_COLUMN));
        photonBeam.setDefaultTtl(row.getInt(BEAM_DEFAULT_TTL_COLUMN));
        return photonBeam;
    }

    private static PhotonBeamReader getMappedBeamReader(PhotonRow row) {
        PhotonBeamReader photonBeamReader = new PhotonBeamReader();
        photonBeamReader.setClientName(row.getString(CLIENT_NAME_COLUMN));
        photonBeamReader.setBeamUuid(row.getUuid(BEAM_UUID_COLUMN));
        photonBeamReader.setBeamReaderUuid(row.getUuid(BEAM_READER_UUID_COLUMN));
        return photonBeamReader;
    }

    private static PhotonBeamReaderLock getMappedBeamReaderLock(PhotonRow row) {
        PhotonBeamReaderLock photonBeamReaderLock = new PhotonBeamReaderLock();
        photonBeamReaderLock.setLockUuid(row.getUuid(BEAM_READER_LOCK_UUID_COLUMN));
        photonBeamReaderLock.setLockTime(row.getInstant(BEAM_READER_LOCK_TIME_COLUMN));
        return photonBeamReaderLock;
    }

    private static Schema getMappedSchema(PhotonRow row) {
        return new Schema.Parser().parse(row.getString(SCHEMA_COLUMN));
    }
}
