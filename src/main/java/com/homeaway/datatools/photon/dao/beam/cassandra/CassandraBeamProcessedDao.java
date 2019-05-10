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
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import com.homeaway.datatools.photon.dao.beam.BeamProcessedDao;
import com.homeaway.datatools.photon.dao.beam.WriteFuture;
import com.homeaway.datatools.photon.dao.rows.PhotonRowSetFuture;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_PROCESSED_QUEUE_TABLE;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_READER_UUID_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.MESSAGE_KEY_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.PARTITION_TIME_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.WRITE_TIME_COLUMN;
import com.homeaway.datatools.photon.utils.dao.PartitionHelper;
import lombok.extern.slf4j.Slf4j;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

@Slf4j
public class CassandraBeamProcessedDao extends AbstractCassandraDao implements BeamProcessedDao {

    private final PreparedStatement preparedInsertStatement;
    private final PreparedStatement preparedSelectStatement;

    public CassandraBeamProcessedDao(final Session session,
                                     final PartitionHelper partitionHelper) {
        super(session, partitionHelper);
        this.preparedInsertStatement = prepareInsertStatement();
        this.preparedSelectStatement = prepareSelectStatement();
    }

    @Override
    public WriteFuture putProcessedMessage(final UUID photonBeamReaderUuid,
                                           final Instant partitionTime,
                                           final Instant writeTime,
                                           final String messageKey) {
        BoundStatement statement = preparedInsertStatement.bind()
                .setUUID(BEAM_READER_UUID_COLUMN, photonBeamReaderUuid)
                .setString(MESSAGE_KEY_COLUMN, messageKey)
                .setTimestamp(PARTITION_TIME_COLUMN, Timestamp.from(partitionTime))
                .setTimestamp(WRITE_TIME_COLUMN, Timestamp.from(writeTime));

        return getWriteFuture(executeQueryAsync(statement));
    }

    @Override
    public PhotonRowSetFuture getProcessedMessages(final UUID photonBeamReaderUuid,
                                                   final Instant partitionTime) {

        BoundStatement statement = preparedSelectStatement.bind()
                .setUUID(BEAM_READER_UUID_COLUMN, photonBeamReaderUuid)
                .setTimestamp(PARTITION_TIME_COLUMN, Timestamp.from(partitionTime));

        return getRowSetFuture(executeQueryAsync(statement));
    }

    private PreparedStatement prepareInsertStatement() {
        return createPreparedStatement(insertInto(BEAM_PROCESSED_QUEUE_TABLE)
                .value(BEAM_READER_UUID_COLUMN, bindMarker(BEAM_READER_UUID_COLUMN))
                .value(MESSAGE_KEY_COLUMN, bindMarker(MESSAGE_KEY_COLUMN))
                .value(PARTITION_TIME_COLUMN, bindMarker(PARTITION_TIME_COLUMN))
                .value(WRITE_TIME_COLUMN, bindMarker(WRITE_TIME_COLUMN)));
    }

    private PreparedStatement prepareSelectStatement() {
        return createPreparedStatement(select()
                .all()
                .from(BEAM_PROCESSED_QUEUE_TABLE)
                .where(eq(BEAM_READER_UUID_COLUMN, bindMarker(BEAM_READER_UUID_COLUMN)))
                .and(eq(PARTITION_TIME_COLUMN, bindMarker(PARTITION_TIME_COLUMN))));
    }
}
