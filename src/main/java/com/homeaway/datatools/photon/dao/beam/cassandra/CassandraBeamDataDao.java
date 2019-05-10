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
import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.ttl;
import com.google.common.collect.Lists;
import com.homeaway.datatools.photon.dao.beam.BeamDataDao;
import com.homeaway.datatools.photon.dao.beam.BeamDataManifestDao;
import com.homeaway.datatools.photon.dao.beam.WriteFuture;
import com.homeaway.datatools.photon.dao.model.message.PhotonProducerMessage;
import com.homeaway.datatools.photon.dao.rows.PhotonRowSetFuture;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_QUEUE_TABLE;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_UUID_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.MESSAGE_KEY_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.PARTITION_TIME_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.PAYLOAD_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.WRITE_TIME_COLUMN;
import com.homeaway.datatools.photon.utils.dao.PartitionHelper;
import static java.nio.ByteOrder.BIG_ENDIAN;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Slf4j
public class CassandraBeamDataDao extends AbstractCassandraDao implements BeamDataDao {

    private final BeamDataManifestDao beamDataManifestDao;
    private final PreparedStatement preparedSelectStatement;
    private final PreparedStatement preparedInsertStatement;
    private final PreparedStatement preparedDeleteStatement;

    public CassandraBeamDataDao(final Session session,
                                final PartitionHelper partitionHelper) {
        this(session, partitionHelper, new CassandraBeamDataManifestDao(session, partitionHelper));
    }

    public CassandraBeamDataDao(final Session session,
                                final PartitionHelper partitionHelper,
                                final BeamDataManifestDao beamDataManifestDao) {
        super(session, partitionHelper);
        this.beamDataManifestDao = beamDataManifestDao;
        this.preparedSelectStatement = prepareSelectStatement();
        this.preparedInsertStatement = prepareInsertStatement();
        this.preparedDeleteStatement = prepareDeleteStatement();
    }

    @Override
    public int getPartitionSize() {
        return getPartitionHelper().getPartitionSize();
    }

    @Override
    public void setPartitionSize(int partitionSize) {
        getPartitionHelper().setPartitionSize(partitionSize);
    }

    @Override
    public List<WriteFuture> putMessageAsync(PhotonProducerMessage message, Integer messageTtl) {
        return Lists.newArrayList(getWriteFuture(executeQueryAsync(getInsertStatement(message.getBeamUuid(),
                message.getMessageKey(), message.getPayload(), message.getWriteTime(), messageTtl))),
                beamDataManifestDao.putBeamMasterManifest(message.getBeamUuid(), message.getWriteTime(), messageTtl),
                beamDataManifestDao.putBeamDataManifest(message.getBeamUuid(), message.getWriteTime(), messageTtl));
    }

    @Override
    public WriteFuture deleteMessageAsync(UUID beamUuid, Instant writeTime, String messageKey) {
        return getWriteFuture(executeQueryAsync(getDeleteStatement(beamUuid, writeTime, messageKey)));
    }

    @Override
    public PhotonRowSetFuture getQueuePartitionAsync(UUID beamUuid, Instant partition) {
        return getRowSetFuture(executeQueryAsync(getSelectStatement(beamUuid, partition)));
    }

    private BoundStatement getSelectStatement(final UUID beamUuid,
                                         final Instant partition) {
        return preparedSelectStatement
                .bind()
                .setUUID(BEAM_UUID_COLUMN, beamUuid)
                .setTimestamp(PARTITION_TIME_COLUMN, Timestamp.from(partition));
    }

    private BoundStatement getInsertStatement(UUID beamUuid, String messageKey, byte[] payload, Instant writeTime, Integer messageTtl) {
        return preparedInsertStatement
                .bind()
                .setUUID(BEAM_UUID_COLUMN, beamUuid)
                .setString(MESSAGE_KEY_COLUMN, messageKey)
                .setBytes(PAYLOAD_COLUMN, (ByteBuffer)ByteBuffer.wrap(payload).order(BIG_ENDIAN).rewind())
                .setTimestamp(WRITE_TIME_COLUMN, Timestamp.from(writeTime))
                .setTimestamp(PARTITION_TIME_COLUMN, Timestamp.from(getPartitionHelper().getPartitionKey(writeTime)))
                .setInt("ttl", Optional.ofNullable(messageTtl).orElse(0));
    }

    private BoundStatement getDeleteStatement(UUID beamUuid, Instant writeTime, String messageKey) {
        return preparedDeleteStatement.bind()
                .setUUID(BEAM_UUID_COLUMN, beamUuid)
                .setTimestamp(PARTITION_TIME_COLUMN, Timestamp.from(getPartitionHelper().getPartitionKey(writeTime)))
                .setTimestamp(WRITE_TIME_COLUMN, Timestamp.from(writeTime))
                .setString(MESSAGE_KEY_COLUMN, messageKey);
    }

    private PreparedStatement prepareInsertStatement() {
        return createPreparedStatement(insertInto(BEAM_QUEUE_TABLE)
                .value(BEAM_UUID_COLUMN, bindMarker(BEAM_UUID_COLUMN))
                .value(MESSAGE_KEY_COLUMN, bindMarker(MESSAGE_KEY_COLUMN))
                .value(PAYLOAD_COLUMN, bindMarker(PAYLOAD_COLUMN))
                .value(WRITE_TIME_COLUMN, bindMarker(WRITE_TIME_COLUMN))
                .value(PARTITION_TIME_COLUMN, bindMarker(PARTITION_TIME_COLUMN))
                .using(ttl(bindMarker("ttl"))));
    }

    private PreparedStatement prepareSelectStatement() {
        return createPreparedStatement(select()
                .all()
                .from(BEAM_QUEUE_TABLE)
                .where(eq(BEAM_UUID_COLUMN, bindMarker(BEAM_UUID_COLUMN)))
                .and(eq(PARTITION_TIME_COLUMN, bindMarker(PARTITION_TIME_COLUMN))));
    }

    private PreparedStatement prepareDeleteStatement() {
        return createPreparedStatement(delete()
                .from(BEAM_QUEUE_TABLE)
                .where(eq(BEAM_UUID_COLUMN, bindMarker(BEAM_UUID_COLUMN)))
                .and(eq(PARTITION_TIME_COLUMN, bindMarker(PARTITION_TIME_COLUMN)))
                .and(eq(WRITE_TIME_COLUMN, bindMarker(WRITE_TIME_COLUMN)))
                .and(eq(MESSAGE_KEY_COLUMN, bindMarker(MESSAGE_KEY_COLUMN))));
    }
}
