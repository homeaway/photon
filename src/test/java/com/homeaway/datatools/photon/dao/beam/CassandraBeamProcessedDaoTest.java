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
package com.homeaway.datatools.photon.dao.beam;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import com.homeaway.datatools.photon.dao.beam.cassandra.CassandraBeamProcessedDao;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;
import com.homeaway.datatools.photon.dao.rows.PhotonRowSet;
import com.homeaway.datatools.photon.dao.rows.PhotonRowSetFuture;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_READER_UUID_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.MESSAGE_KEY_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.PARTITION_TIME_COLUMN;
import com.homeaway.datatools.photon.utils.dao.DefaultPartitionHelper;
import com.homeaway.datatools.photon.utils.dao.PartitionHelper;
import com.homeaway.datatools.photon.utils.test.cassandra.testing.EmbeddedCassandraRule;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.concurrent.ExecutionException;

@Slf4j
public class CassandraBeamProcessedDaoTest {

    private static final String BEAM_PROCESSED_QUEUE_TABLE = "beam_processed_data";
    private static final ConsistencyLevel CONSISTENCY_LEVEL = ConsistencyLevel.LOCAL_QUORUM;
    private BeamProcessedDao beamProcessedDao;
    private Session session;
    private PartitionHelper partitionHelper;

    @Rule
    public EmbeddedCassandraRule embeddedCassandraRule = new EmbeddedCassandraRule("cassandra/create_keyspace.cql",
            "cassandra/beam_processed_data.cql");

    @Before
    public void init() {
        this.session = embeddedCassandraRule.cluster().connect("photon");
        this.partitionHelper = new DefaultPartitionHelper(100);
        this.beamProcessedDao = new CassandraBeamProcessedDao(session, partitionHelper);
    }

    @Test
    public void testPutProcessedMessage() {
        Instant writeTime = Instant.now().minusMillis(10);
        String messageKey = "TestProcessedMessage";
        PhotonBeamReader reader = CassandraBeamDaoTestHelper.buildPhotonBeamReader();
        WriteFuture future = beamProcessedDao.putProcessedMessage(reader.getBeamReaderUuid(), partitionHelper.getPartitionKey(writeTime), writeTime, messageKey);
        try {
            future.get();
            ResultSet rs = session.execute(getSelectProcessedStatement(reader, writeTime));
            Assert.assertEquals(1, rs.getAvailableWithoutFetching());
        } catch (InterruptedException | ExecutionException e) {
            log.error("Could not write processed message", e);
        }
    }

    @Test
    public void testGetProcessedMessage() {
        Instant writeTime = Instant.now().minusMillis(10);
        String messageKey = "TestProcessedMessage";
        PhotonBeamReader reader = CassandraBeamDaoTestHelper.buildPhotonBeamReader();
        WriteFuture future = beamProcessedDao.putProcessedMessage(reader.getBeamReaderUuid(), partitionHelper.getPartitionKey(writeTime), writeTime, messageKey);
        try {
            future.get();
            PhotonRowSetFuture getFuture = beamProcessedDao.getProcessedMessages(reader.getBeamReaderUuid(), partitionHelper.getPartitionKey(writeTime));
            PhotonRowSet resultSet = getFuture.get();
            Assert.assertEquals(1, resultSet.getSize());
            Assert.assertEquals(messageKey, resultSet.one().getString(MESSAGE_KEY_COLUMN));
        } catch (InterruptedException | ExecutionException e) {
            log.error("Could not write processed message", e);
        }
    }

    private Statement getSelectProcessedStatement(final PhotonBeamReader photonBeamReader,
                                                  final Instant writeTime) {
        return select()
                .all()
                .from(BEAM_PROCESSED_QUEUE_TABLE)
                .where(eq(BEAM_READER_UUID_COLUMN, photonBeamReader.getBeamReaderUuid()))
                .and(eq(PARTITION_TIME_COLUMN, Timestamp.from(partitionHelper.getPartitionKey(writeTime))))
                .setConsistencyLevel(CONSISTENCY_LEVEL);
    }

}
