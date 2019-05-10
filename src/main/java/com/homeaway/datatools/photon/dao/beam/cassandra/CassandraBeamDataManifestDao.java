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
import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.ttl;
import com.homeaway.datatools.photon.dao.beam.BeamDataManifestDao;
import com.homeaway.datatools.photon.dao.beam.WriteFuture;
import com.homeaway.datatools.photon.dao.rows.PhotonRowSetFuture;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_DATA_MANIFEST_TABLE;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_DATA_MASTER_MANIFEST_TABLE;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_UUID_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.MANIFEST_TIME_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.MASTER_MANIFEST_TIME_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.PARTITION_TIME_COLUMN;
import com.homeaway.datatools.photon.utils.dao.PartitionHelper;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public class CassandraBeamDataManifestDao extends AbstractCassandraDao implements BeamDataManifestDao {

    private final PreparedStatement masterPreparedSelectStatement;
    private final PreparedStatement masterPreparedInsertStatement;
    private final PreparedStatement preparedSelectStatement;
    private final PreparedStatement preparedInsertStatement;


    public CassandraBeamDataManifestDao(final Session session,
                                        final PartitionHelper partitionHelper) {
        super(session, partitionHelper);
        this.masterPreparedSelectStatement = prepareMasterSelectStatement();
        this.masterPreparedInsertStatement = prepareMasterInsertStatement();
        this.preparedSelectStatement = prepareSelectStatement();
        this.preparedInsertStatement = prepareInsertStatement();
    }

    @Override
    public PhotonRowSetFuture getBeamMasterManifest(UUID beamUuid, Instant masterManifestTime, Instant minManifestTime, Instant maxManifestTime) {
        return getRowSetFuture(executeQueryAsync(getMasterSelectStatement(beamUuid, masterManifestTime, minManifestTime, maxManifestTime)));
    }

    @Override
    public WriteFuture putBeamMasterManifest(UUID beamUuid, Instant writeTime, Integer messageTtl) {
        return getWriteFuture(executeQueryAsync(getMasterInsertStatement(beamUuid, writeTime, messageTtl)));
    }

    @Override
    public PhotonRowSetFuture getBeamDataManifest(UUID beamUuid, Instant manifestTime, Instant maxPartitionTime) {
        return getRowSetFuture(executeQueryAsync(getSelectStatement(beamUuid, manifestTime, maxPartitionTime)));
    }

    @Override
    public WriteFuture putBeamDataManifest(UUID beamUuid, Instant writeTime, Integer messageTtl) {
        return getWriteFuture(executeQueryAsync(getInsertStatement(beamUuid, writeTime, messageTtl)));
    }

    private BoundStatement getMasterSelectStatement(UUID beamUuid, Instant masterManifestTime, Instant minManifestTime, Instant maxManifestTime) {
        return masterPreparedSelectStatement.bind()
                .setUUID(BEAM_UUID_COLUMN, beamUuid)
                .setTimestamp(MASTER_MANIFEST_TIME_COLUMN, Timestamp.from(getPartitionHelper().getMasterManifestKey(masterManifestTime)))
                .setTimestamp(MANIFEST_TIME_COLUMN, Timestamp.from(getPartitionHelper().getManifestKey(minManifestTime)))
                .setTimestamp("END_MANIFEST_TIME", Timestamp.from(getPartitionHelper().getManifestKey(maxManifestTime)));
    }

    private BoundStatement getMasterInsertStatement(UUID beamUuid, Instant writeTime, Integer messageTtl) {
        return masterPreparedInsertStatement.bind()
                .setUUID(BEAM_UUID_COLUMN, beamUuid)
                .setTimestamp(MASTER_MANIFEST_TIME_COLUMN, Timestamp.from(getPartitionHelper().getMasterManifestKey(writeTime)))
                .setTimestamp(MANIFEST_TIME_COLUMN, Timestamp.from(getPartitionHelper().getManifestKey(writeTime)))
                .setInt("ttl", Optional.ofNullable(messageTtl).orElse(0));
    }

    private BoundStatement getSelectStatement(UUID beamUuid, Instant manifestTime, Instant maxPartitionTime) {
        return preparedSelectStatement.bind()
                .setUUID(BEAM_UUID_COLUMN, beamUuid)
                .setTimestamp(MANIFEST_TIME_COLUMN, Timestamp.from(getPartitionHelper().getManifestKey(manifestTime)))
                .setTimestamp(PARTITION_TIME_COLUMN, Timestamp.from(maxPartitionTime.plusSeconds(1)));
    }

    private BoundStatement getInsertStatement(UUID beamUuid, Instant writeTime, Integer messageTtl) {
        return preparedInsertStatement.bind()
                .setUUID(BEAM_UUID_COLUMN, beamUuid)
                .setTimestamp(MANIFEST_TIME_COLUMN, Timestamp.from(getPartitionHelper().getManifestKey(writeTime)))
                .setTimestamp(PARTITION_TIME_COLUMN, Timestamp.from(getPartitionHelper().getPartitionKey(writeTime)))
                .setInt("ttl", Optional.ofNullable(messageTtl).orElse(0));
    }

    private PreparedStatement prepareSelectStatement() {
        return createPreparedStatement(select()
                .all()
                .from(BEAM_DATA_MANIFEST_TABLE)
                .where(eq(BEAM_UUID_COLUMN, bindMarker(BEAM_UUID_COLUMN)))
                .and(eq(MANIFEST_TIME_COLUMN, bindMarker(MANIFEST_TIME_COLUMN)))
                .and(lt(PARTITION_TIME_COLUMN, bindMarker(PARTITION_TIME_COLUMN))));
    }

    private PreparedStatement prepareInsertStatement() {
        return createPreparedStatement(insertInto(BEAM_DATA_MANIFEST_TABLE)
                .value(BEAM_UUID_COLUMN, bindMarker(BEAM_UUID_COLUMN))
                .value(MANIFEST_TIME_COLUMN, bindMarker(MANIFEST_TIME_COLUMN))
                .value(PARTITION_TIME_COLUMN, bindMarker(PARTITION_TIME_COLUMN))
                .using(ttl(bindMarker("ttl"))));
    }

    private PreparedStatement prepareMasterSelectStatement() {
        return createPreparedStatement(select()
                .all()
                .from(BEAM_DATA_MASTER_MANIFEST_TABLE)
                .where(eq(BEAM_UUID_COLUMN, bindMarker(BEAM_UUID_COLUMN)))
                .and(eq(MASTER_MANIFEST_TIME_COLUMN, bindMarker(MASTER_MANIFEST_TIME_COLUMN)))
                .and(gte(MANIFEST_TIME_COLUMN, bindMarker(MANIFEST_TIME_COLUMN)))
                .and(lte(MANIFEST_TIME_COLUMN, bindMarker("END_MANIFEST_TIME"))));
    }

    private PreparedStatement prepareMasterInsertStatement() {
        return createPreparedStatement(insertInto(BEAM_DATA_MASTER_MANIFEST_TABLE)
                .value(BEAM_UUID_COLUMN, bindMarker(BEAM_UUID_COLUMN))
                .value(MASTER_MANIFEST_TIME_COLUMN, bindMarker(MASTER_MANIFEST_TIME_COLUMN))
                .value(MANIFEST_TIME_COLUMN, bindMarker(MANIFEST_TIME_COLUMN))
                .using(ttl(bindMarker("ttl"))));
    }
}
