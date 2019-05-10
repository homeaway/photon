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
package com.homeaway.datatools.photon.dao.beam.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import com.homeaway.datatools.photon.dao.beam.BeamReaderLockDao;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_READER_LOCK_TIME_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_READER_LOCK_UUID_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_READER_TABLE;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_UUID_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.CLIENT_NAME_COLUMN;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReaderLock;
import lombok.extern.slf4j.Slf4j;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.UUID;

@Slf4j
public class CassandraBeamReaderLockDao extends AbstractCassandraDao implements BeamReaderLockDao {

    private final Duration lockThreshold;
    private final PreparedStatement preparedUpdateStatement;
    private final PreparedStatement preparedSelectStatement;

    public CassandraBeamReaderLockDao(final Session session,
                                      final Duration lockThreshold) {
        super(session);
        this.lockThreshold = lockThreshold;
        this.preparedUpdateStatement = prepareUpdateStatement();
        this.preparedSelectStatement = prepareSelectStatement();
    }

    @Override
    public PhotonBeamReaderLock putBeamReaderLock(String clientName, UUID beamUuid, PhotonBeamReaderLock photonBeamReaderLock) {
        putLock(clientName, beamUuid, photonBeamReaderLock);
        try {
            Thread.sleep(lockThreshold.dividedBy(4).toMillis());
        } catch (InterruptedException e) {
            log.warn("Photon beam read dao thread interrupted.", e);
        }
        return getLock(clientName, beamUuid);
    }

    @Override
    public void updateLock(PhotonBeamReader photonBeamReader) {
        if (photonBeamReader.getPhotonBeamReaderLock().isPresent()) {
            if (photonBeamReader.getPhotonBeamReaderLock().isPresent()) {
                putLock(photonBeamReader.getClientName(), photonBeamReader.getBeamUuid(), photonBeamReader.getPhotonBeamReaderLock().get());
            }
        }
    }

    @Override
    public Optional<PhotonBeamReaderLock> getAvailablePhotonBeamLock(PhotonBeamReader photonBeamReader) {
    Instant lockTimeThreshold = Instant.now().minus(lockThreshold.toMillis(), ChronoUnit.MILLIS);
        PhotonBeamReaderLock photonBeamReaderLock = getLock(photonBeamReader.getClientName(), photonBeamReader.getBeamUuid());
        if (photonBeamReaderLock != null) {
            if (photonBeamReaderLock.getLockTime() == null
                    || photonBeamReaderLock.getLockTime().isBefore(lockTimeThreshold)) {
                return Optional.of(photonBeamReaderLock);
            }
        }
        return Optional.empty();
    }

    @Override
    public PhotonBeamReaderLock getPhotonBeamLock(PhotonBeamReader photonBeamReader) {
        return getLock(photonBeamReader.getClientName(), photonBeamReader.getBeamUuid());
    }

    private void putLock(String clientName, UUID beamUuid, PhotonBeamReaderLock photonBeamReaderLock) {
        BoundStatement statement = preparedUpdateStatement.bind()
                .setTimestamp(BEAM_READER_LOCK_TIME_COLUMN, Timestamp.from(photonBeamReaderLock.getLockTime()))
                .setUUID(BEAM_READER_LOCK_UUID_COLUMN, photonBeamReaderLock.getLockUuid())
                .setString(CLIENT_NAME_COLUMN, clientName)
                .setUUID(BEAM_UUID_COLUMN, beamUuid);

        executeQuery(statement);
    }

    private PhotonBeamReaderLock getLock(String clientName, UUID beamUuid) {
        BoundStatement statement = preparedSelectStatement.bind()
                .setString(CLIENT_NAME_COLUMN, clientName)
                .setUUID(BEAM_UUID_COLUMN, beamUuid);

        return getMappedEntity(getRowSet(executeQuery(statement)).one(),
                PhotonBeamReaderLock.class);
    }

    private PreparedStatement prepareUpdateStatement() {
        return createPreparedStatement(QueryBuilder.update(BEAM_READER_TABLE)
                .with(set(BEAM_READER_LOCK_TIME_COLUMN, QueryBuilder.bindMarker(BEAM_READER_LOCK_TIME_COLUMN)))
                .and(set(BEAM_READER_LOCK_UUID_COLUMN, QueryBuilder.bindMarker(BEAM_READER_LOCK_UUID_COLUMN)))
                .where(eq(CLIENT_NAME_COLUMN, QueryBuilder.bindMarker(CLIENT_NAME_COLUMN)))
                .and(eq(BEAM_UUID_COLUMN, QueryBuilder.bindMarker(BEAM_UUID_COLUMN))));
    }

    private PreparedStatement prepareSelectStatement() {
        return createPreparedStatement(QueryBuilder
                .select()
                .column(BEAM_READER_LOCK_UUID_COLUMN)
                .column(BEAM_READER_LOCK_TIME_COLUMN)
                .from(BEAM_READER_TABLE)
                .where(eq(CLIENT_NAME_COLUMN, QueryBuilder.bindMarker(CLIENT_NAME_COLUMN)))
                .and(eq(BEAM_UUID_COLUMN, QueryBuilder.bindMarker(BEAM_UUID_COLUMN))));
    }
}
