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
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.homeaway.datatools.photon.dao.beam.BeamDao;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import com.homeaway.datatools.photon.dao.rows.PhotonRow;
import com.homeaway.datatools.photon.dao.rows.PhotonRowSet;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_BY_NAME_TABLE;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_BY_UUID_TABLE;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_DEFAULT_TTL_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_NAME_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_UUID_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.START_DATE_COLUMN;
import lombok.extern.slf4j.Slf4j;

import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Slf4j
public class CassandraBeamDao extends AbstractCassandraDao implements BeamDao {

    private final PreparedStatement preparedByNameInsert;
    private final PreparedStatement preparedByUuidInsert;
    private final PreparedStatement preparedByNameSelect;
    private final PreparedStatement preparedByUuidSelect;

    public CassandraBeamDao(final Session session) {
        super(session);
        this.preparedByNameInsert = prepareInsertStatement(BEAM_BY_NAME_TABLE);
        this.preparedByUuidInsert = prepareInsertStatement(BEAM_BY_UUID_TABLE);
        this.preparedByNameSelect = prepareSelectByNameStatement();
        this.preparedByUuidSelect = prepareSelectByUuidStatement();
    }

    @Override
    public void putBeam(PhotonBeam photonBeam) {
        List<ResultSetFuture> futureList = Lists.newArrayList();
        BoundStatement tableByNameInsert = preparedByNameInsert.bind()
                .setString(BEAM_NAME_COLUMN, photonBeam.getBeamName())
                .setUUID(BEAM_UUID_COLUMN, photonBeam.getBeamUuid())
                .setTimestamp(START_DATE_COLUMN, Timestamp.from(photonBeam.getStartDate()))
                .setInt(BEAM_DEFAULT_TTL_COLUMN, Optional.ofNullable(photonBeam.getDefaultTtl()).orElse(0));

        BoundStatement tableByUuidInsert = preparedByUuidInsert.bind()
                .setString(BEAM_NAME_COLUMN, photonBeam.getBeamName())
                .setUUID(BEAM_UUID_COLUMN, photonBeam.getBeamUuid())
                .setTimestamp(START_DATE_COLUMN, Timestamp.from(photonBeam.getStartDate()))
                .setInt(BEAM_DEFAULT_TTL_COLUMN, Optional.ofNullable(photonBeam.getDefaultTtl()).orElse(0));

        futureList.add(executeQueryAsync(tableByNameInsert));
        futureList.add(executeQueryAsync(tableByUuidInsert));

        try {
            Futures.allAsList(futureList).get();
        } catch (ExecutionException | InterruptedException e) {
            log.error("Could not put photonBeam {}", photonBeam, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Queue<PhotonBeam> getBeamByName(String beamName) {
        BoundStatement statement = preparedByNameSelect
                .bind()
                .setString(BEAM_NAME_COLUMN, beamName);

        PhotonRowSet rowSet = getRowSet(executeQuery(statement));
        Queue<PhotonBeam> beams = Lists.newLinkedList();
        if (rowSet.getSize() > 0) {
            for(PhotonRow row : rowSet) {
                beams.add(getMappedEntity(row, PhotonBeam.class));
            }
        }
        return beams;
    }

    @Override
    public Optional<PhotonBeam> getBeamByUuid(UUID beamUuid) {
        BoundStatement statement = preparedByUuidSelect
                .bind()
                .setUUID(BEAM_UUID_COLUMN, beamUuid);

        return Optional.ofNullable(getRowSet(executeQuery(statement)).one())
                .map(r -> getMappedEntity(r, PhotonBeam.class));
    }

    private PreparedStatement prepareInsertStatement(String tableName) {
        return createPreparedStatement(insertInto(tableName)
                .value(BEAM_NAME_COLUMN, bindMarker(BEAM_NAME_COLUMN))
                .value(BEAM_UUID_COLUMN, bindMarker(BEAM_UUID_COLUMN))
                .value(START_DATE_COLUMN, bindMarker(START_DATE_COLUMN))
                .value(BEAM_DEFAULT_TTL_COLUMN, bindMarker(BEAM_DEFAULT_TTL_COLUMN)));
    }

    private PreparedStatement prepareSelectByNameStatement() {
        return createPreparedStatement(select()
                .all()
                .from(BEAM_BY_NAME_TABLE)
                .where(eq(BEAM_NAME_COLUMN, bindMarker(BEAM_NAME_COLUMN))));

    }

    private PreparedStatement prepareSelectByUuidStatement() {
        return createPreparedStatement(select()
                .all()
                .from(BEAM_BY_UUID_TABLE)
                .where(eq(BEAM_UUID_COLUMN, bindMarker(BEAM_UUID_COLUMN))));
    }
}
