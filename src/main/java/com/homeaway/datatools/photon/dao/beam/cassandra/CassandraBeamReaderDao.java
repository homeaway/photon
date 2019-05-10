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
import com.datastax.driver.core.SimpleStatement;
import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import com.homeaway.datatools.photon.dao.beam.BeamReaderDao;
import com.homeaway.datatools.photon.dao.beam.WriteFuture;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_READER_TABLE;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_READER_UUID_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_UUID_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.CLIENT_NAME_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.CLUSTER_NOW_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.WATERMARK_COLUMN;
import lombok.extern.slf4j.Slf4j;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;

@Slf4j
public class CassandraBeamReaderDao extends AbstractCassandraDao implements BeamReaderDao {

    private final PreparedStatement preparedClusterTimeStatement;
    private final PreparedStatement preparedInsertStatement;
    private final PreparedStatement preparedSelectStatement;
    private final PreparedStatement preparedWatermarkUpdateStatement;
    private final PreparedStatement preparedWatermarkSelectStatement;

    public CassandraBeamReaderDao(final Session session) {
        super(session);
        this.preparedClusterTimeStatement = prepareClusterTimeStatement();
        this.preparedInsertStatement = prepareInsertStatement();
        this.preparedSelectStatement = prepareSelectStatement();
        this.preparedWatermarkUpdateStatement = prepareWatermarkUpdateStatement();
        this.preparedWatermarkSelectStatement = prepareWatermarkSelectStatement();
    }

    @Override
    public void putBeamReader(PhotonBeamReader photonBeamReader) {
        BoundStatement statement = preparedInsertStatement.bind()
                .setString(CLIENT_NAME_COLUMN, photonBeamReader.getClientName())
                .setUUID(BEAM_UUID_COLUMN, photonBeamReader.getBeamUuid())
                .setUUID(BEAM_READER_UUID_COLUMN, photonBeamReader.getBeamReaderUuid());

        try {
            executeQuery(statement);
        } catch (Exception e) {
            log.error("Could not put beam reader {}.", photonBeamReader, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<PhotonBeamReader> getBeamReaderByClientNameBeamUuid(String clientName, UUID beamUuid) {
        BoundStatement statement = preparedSelectStatement.bind()
                .setString(CLIENT_NAME_COLUMN, clientName)
                .setUUID(BEAM_UUID_COLUMN, beamUuid);

        return Optional.ofNullable(getRowSet(executeQuery(statement)).one())
                .map(r -> getMappedEntity(r, PhotonBeamReader.class));
    }

    @Override
    public WriteFuture updateWatermark(final PhotonBeamReader photonBeamReader) {
        BoundStatement statement = preparedWatermarkUpdateStatement.bind()
                .setTimestamp(WATERMARK_COLUMN, Timestamp.from(photonBeamReader.getWaterMark()))
                .setString(CLIENT_NAME_COLUMN, photonBeamReader.getClientName())
                .setUUID(BEAM_UUID_COLUMN, photonBeamReader.getBeamUuid());
        return getWriteFuture(executeQueryAsync(statement));
    }

    @Override
    public Instant getCurrentClusterTime() {
        return executeQuery(preparedClusterTimeStatement.bind()).one().getTimestamp(CLUSTER_NOW_COLUMN).toInstant();
    }

    @Override
    public Optional<Instant> getWatermark(PhotonBeamReader photonBeamReader) {
        BoundStatement statement = preparedWatermarkSelectStatement.bind()
                .setString(CLIENT_NAME_COLUMN, photonBeamReader.getClientName())
                .setUUID(BEAM_UUID_COLUMN, photonBeamReader.getBeamUuid());

        Date waterMark = executeQuery(statement).one().getTimestamp(WATERMARK_COLUMN);
        if (waterMark != null) {
            return Optional.of(waterMark.toInstant());
        }
        return Optional.empty();
    }

    private PreparedStatement prepareClusterTimeStatement() {
        return createPreparedStatement(new SimpleStatement("select toTimestamp(now()) as now from system.local;"));
    }

    private PreparedStatement prepareInsertStatement() {
        return createPreparedStatement(insertInto(BEAM_READER_TABLE)
                .value(CLIENT_NAME_COLUMN, bindMarker(CLIENT_NAME_COLUMN))
                .value(BEAM_UUID_COLUMN, bindMarker(BEAM_UUID_COLUMN))
                .value(BEAM_READER_UUID_COLUMN, bindMarker(BEAM_READER_UUID_COLUMN)));
    }

    private PreparedStatement prepareSelectStatement() {
        return createPreparedStatement(select()
                .column(CLIENT_NAME_COLUMN)
                .column(BEAM_UUID_COLUMN)
                .column(BEAM_READER_UUID_COLUMN)
                .from(BEAM_READER_TABLE)
                .where(eq(CLIENT_NAME_COLUMN, bindMarker(CLIENT_NAME_COLUMN)))
                .and(eq(BEAM_UUID_COLUMN, bindMarker(BEAM_UUID_COLUMN))));
    }

    private PreparedStatement prepareWatermarkUpdateStatement() {
        return createPreparedStatement(update(BEAM_READER_TABLE)
                .with(set(WATERMARK_COLUMN, bindMarker(WATERMARK_COLUMN)))
                .where(eq(CLIENT_NAME_COLUMN, bindMarker(CLIENT_NAME_COLUMN)))
                .and(eq(BEAM_UUID_COLUMN, bindMarker(BEAM_UUID_COLUMN))));
    }

    private PreparedStatement prepareWatermarkSelectStatement() {
        return createPreparedStatement(select()
                .column(WATERMARK_COLUMN)
                .from(BEAM_READER_TABLE)
                .where(eq(CLIENT_NAME_COLUMN, bindMarker(CLIENT_NAME_COLUMN)))
                .and(eq(BEAM_UUID_COLUMN, bindMarker(BEAM_UUID_COLUMN))));
    }
}
