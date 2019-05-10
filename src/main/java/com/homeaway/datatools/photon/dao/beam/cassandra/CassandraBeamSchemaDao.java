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
import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import com.homeaway.datatools.photon.dao.beam.BeamSchemaDao;
import com.homeaway.datatools.photon.dao.beam.WriteFuture;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_SCHEMA_TABLE;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_UUID_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.SCHEMA_COLUMN;
import org.apache.avro.Schema;

import java.util.Optional;
import java.util.UUID;

public class CassandraBeamSchemaDao extends AbstractCassandraDao implements BeamSchemaDao {
    private final PreparedStatement insertStatement;
    private final PreparedStatement getStatement;

    public CassandraBeamSchemaDao(final Session session) {
        super(session);
        insertStatement = prepareInsertStatement();
        getStatement = prepareGetStatement();
    }

    @Override
    public Optional<Schema> getSchemaByBeamUuid(UUID beamUuid) {
        return Optional.ofNullable(getRowSet(executeQuery(bindGetStatement(beamUuid))).one())
                .map(r -> getMappedEntity(r, Schema.class));
    }

    @Override
    public WriteFuture putBeamSchema(PhotonBeam beam, Schema schema) {
        return getWriteFuture(executeQueryAsync(bindInsertStatement(beam.getBeamUuid(), schema.toString())));
    }

    private BoundStatement bindGetStatement(UUID beamUuid) {
        return getStatement.bind()
                .setUUID(BEAM_UUID_COLUMN, beamUuid);
    }

    private BoundStatement bindInsertStatement(UUID beamUuid, String schema) {
        return insertStatement.bind()
                .setUUID(BEAM_UUID_COLUMN, beamUuid)
                .setString(SCHEMA_COLUMN, schema);
    }

    private PreparedStatement prepareInsertStatement() {
        return createPreparedStatement(insertInto(BEAM_SCHEMA_TABLE)
                .value(BEAM_UUID_COLUMN, bindMarker(BEAM_UUID_COLUMN))
                .value(SCHEMA_COLUMN, bindMarker(SCHEMA_COLUMN)));
    }

    private PreparedStatement prepareGetStatement() {
        return createPreparedStatement(QueryBuilder.select()
                .column(SCHEMA_COLUMN)
                .from(BEAM_SCHEMA_TABLE)
                .where(eq(BEAM_UUID_COLUMN, bindMarker(BEAM_UUID_COLUMN))));
    }
}
