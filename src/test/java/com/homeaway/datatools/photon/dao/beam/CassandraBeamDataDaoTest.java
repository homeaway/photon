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
import com.datastax.driver.core.querybuilder.QueryBuilder;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.homeaway.datatools.photon.dao.beam.cassandra.CassandraBeamDataDao;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import com.homeaway.datatools.photon.dao.model.message.PhotonProducerMessage;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;
import com.homeaway.datatools.photon.dao.rows.PhotonRowSetFuture;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_UUID_COLUMN;
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
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Slf4j
public class CassandraBeamDataDaoTest {

    private static final String BEAM_QUEUE_TABLE = "beam_data";
    private static final ConsistencyLevel CONSISTENCY_LEVEL = ConsistencyLevel.LOCAL_QUORUM;
    private static final int FETCH_SIZE = 300;
    private BeamDataDao beamDataDao;
    private PartitionHelper partitionHelper;
    private Session session;

    @Rule
    public EmbeddedCassandraRule embeddedCassandraRule = new EmbeddedCassandraRule("cassandra/create_keyspace.cql",
            "cassandra/beam_data.cql", "cassandra/beam_processed_data.cql", "cassandra/beam_data_manifest.cql",
            "cassandra/beam_data_master_manifest.cql");

    @Before
    public void init() {
        this.session = embeddedCassandraRule.cluster().connect("photon");
        this.partitionHelper = new DefaultPartitionHelper(100);
        this.beamDataDao = new CassandraBeamDataDao(session, partitionHelper);
    }

    @Test
    public void testPutMessage()  throws InterruptedException, ExecutionException {
        PhotonBeam beam = CassandraBeamDaoTestHelper.buildPhotonBeam();
        PhotonBeamReader reader = new PhotonBeamReader();
        reader.setBeamUuid(beam.getBeamUuid());
        Instant now = Instant.now();
        PhotonProducerMessage message = new PhotonProducerMessage(beam.getBeamUuid(), "TestMessageKey", UUID.randomUUID().toString().getBytes(), Instant.now());
        Futures.allAsList(beamDataDao.putMessageAsync(message, beam.getDefaultTtl())).get();
        List<ResultSet> resultSets = Lists.newArrayList();
        for(int i = 0; i < now.until(now.plusMillis(500), ChronoUnit.MILLIS); i+=100) {
            resultSets.add(session.execute(getSelectQueueStatement(reader, partitionHelper.getPartitionKey(now.plusMillis(i)))));
        }
        Assert.assertEquals(1, resultSets.stream()
                .mapToInt(rs -> rs.getAvailableWithoutFetching())
                .sum());
    }

    @Test
    public void testPutMessageAsync() {
        PhotonBeam beam = CassandraBeamDaoTestHelper.buildPhotonBeam();
        PhotonBeamReader reader = new PhotonBeamReader();
        reader.setBeamUuid(beam.getBeamUuid());
        Instant now = Instant.now();
        PhotonProducerMessage message = new PhotonProducerMessage(beam.getBeamUuid(), "TestMessageKeyAsync", UUID.randomUUID().toString().getBytes(), Instant.now());
        List<WriteFuture> futures = beamDataDao.putMessageAsync(message, beam.getDefaultTtl());
        try {
            Futures.successfulAsList(futures).get();
            List<ResultSet> resultSets = Lists.newArrayList();
            for(int i = 0; i < now.until(now.plusMillis(500), ChronoUnit.MILLIS); i+=100) {
                resultSets.add(session.execute(getSelectQueueStatement(reader, partitionHelper.getPartitionKey(now.plusMillis(i)))));
            }
            Assert.assertEquals(1, resultSets.stream()
                    .mapToInt(rs -> rs.getAvailableWithoutFetching())
                    .sum());
        } catch (InterruptedException | ExecutionException e) {
            log.error("Could not get result set from future");
        }
    }

    @Test
    public void testGetQueueMessages() throws InterruptedException, ExecutionException {
        PhotonBeam beam = CassandraBeamDaoTestHelper.buildPhotonBeam();
        PhotonBeamReader reader = new PhotonBeamReader();
        reader.setBeamUuid(beam.getBeamUuid());
        reader.setBeamReaderUuid(UUID.randomUUID());
        Instant now = Instant.now();
        Futures.allAsList(beamDataDao.putMessageAsync(new PhotonProducerMessage(beam.getBeamUuid(), "TestMessageKey", UUID.randomUUID().toString().getBytes(), Instant.now()), beam.getDefaultTtl())).get();
        Futures.allAsList(beamDataDao.putMessageAsync(new PhotonProducerMessage(beam.getBeamUuid(), "TestMessageKey1", UUID.randomUUID().toString().getBytes(), Instant.now()), beam.getDefaultTtl())).get();
        Futures.allAsList(beamDataDao.putMessageAsync(new PhotonProducerMessage(beam.getBeamUuid(), "TestMessageKey2", UUID.randomUUID().toString().getBytes(), Instant.now()), beam.getDefaultTtl())).get();
        Futures.allAsList(beamDataDao.putMessageAsync(new PhotonProducerMessage(beam.getBeamUuid(), "TestMessageKey3", UUID.randomUUID().toString().getBytes(), Instant.now()), beam.getDefaultTtl())).get();
        List<PhotonRowSetFuture> results = Lists.newArrayList();
        for(int i = 0; i < now.until(now.plusMillis(600), ChronoUnit.MILLIS); i+=100) {
            results.add(beamDataDao.getQueuePartitionAsync(reader.getBeamUuid(), partitionHelper.getPartitionKey(now.plusMillis(i))));
        }
        Assert.assertEquals(4, results.stream()
                .mapToInt(r -> {
                    try {
                        return r.get().getSize();
                    } catch (InterruptedException | ExecutionException e) {
                    }
                    return 0;
                })
                .sum());
    }

    @Test
    public void testPutMessageWithWriteTimeAsync() {
        Instant writeTime = Instant.now().plusSeconds(30);
        PhotonBeam beam = CassandraBeamDaoTestHelper.buildPhotonBeam();
        PhotonBeamReader reader = new PhotonBeamReader();
        reader.setBeamUuid(beam.getBeamUuid());
        List<WriteFuture> futures = beamDataDao.putMessageAsync(new PhotonProducerMessage(beam.getBeamUuid(),
                "TestMessageKeyWithWriteTimeAsync", UUID.randomUUID().toString().getBytes(), writeTime), beam.getDefaultTtl());
        try {
            Futures.successfulAsList(futures).get();
            ResultSet resultSet = session.execute(getSelectQueueStatement(reader, partitionHelper.getPartitionKey(writeTime)));
            Assert.assertEquals(1, resultSet.getAvailableWithoutFetching());
        } catch (InterruptedException | ExecutionException e) {
            log.error("Could not get result set from future");
        }
    }

    @Test
    public void testPutMessageWithWriteTime() throws InterruptedException, ExecutionException {
        Instant writeTime = Instant.now().plusSeconds(30);
        PhotonBeam beam = CassandraBeamDaoTestHelper.buildPhotonBeam();
        PhotonBeamReader reader = new PhotonBeamReader();
        reader.setBeamUuid(beam.getBeamUuid());
        Futures.allAsList(beamDataDao.putMessageAsync(new PhotonProducerMessage(beam.getBeamUuid(),
                "TestMessageKeyWithWriteTime", UUID.randomUUID().toString().getBytes(), writeTime), beam.getDefaultTtl())).get();
        ResultSet resultSet = session.execute(getSelectQueueStatement(reader, partitionHelper.getPartitionKey(writeTime)));
        Assert.assertEquals(1, resultSet.getAvailableWithoutFetching());
    }

    @Test
    public void testPutMessageWithTtl() throws InterruptedException, ExecutionException {
        Instant writeTime = Instant.now();
        PhotonBeam beam = CassandraBeamDaoTestHelper.buildPhotonBeam();
        PhotonBeamReader reader = new PhotonBeamReader();
        reader.setBeamUuid(beam.getBeamUuid());
        Futures.allAsList(beamDataDao.putMessageAsync(new PhotonProducerMessage(beam.getBeamUuid(),
                "TestMessageKeyWithWriteTime", UUID.randomUUID().toString().getBytes(), writeTime), 2)).get();
        ResultSet resultSet = session.execute(getSelectQueueStatement(reader, partitionHelper.getPartitionKey(writeTime)));
        Assert.assertEquals(1, resultSet.getAvailableWithoutFetching());
        try {
            Thread.sleep(2500);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        resultSet = session.execute(getSelectQueueStatement(reader, partitionHelper.getPartitionKey(writeTime)));
        Assert.assertEquals(0, resultSet.getAvailableWithoutFetching());
    }

    @Test
    public void testDeleteMessageAsync() throws InterruptedException, ExecutionException {
        Instant writeTime = Instant.now().plusSeconds(20);
        PhotonBeam beam = CassandraBeamDaoTestHelper.buildPhotonBeam();
        PhotonBeamReader reader = new PhotonBeamReader();
        reader.setBeamUuid(beam.getBeamUuid());
        Futures.allAsList(beamDataDao.putMessageAsync(new PhotonProducerMessage(beam.getBeamUuid(),
                "TestMessageKeyDeleteAsync", UUID.randomUUID().toString().getBytes(), writeTime), beam.getDefaultTtl())).get();
        ResultSet resultSet = session.execute(getSelectQueueStatement(reader, partitionHelper.getPartitionKey(writeTime)));
        Assert.assertEquals(1, resultSet.getAvailableWithoutFetching());
        WriteFuture writeFuture = beamDataDao.deleteMessageAsync(beam.getBeamUuid(), writeTime, "TestMessageKeyDeleteAsync");
        try {
            writeFuture.get();
            resultSet = session.execute(getSelectQueueStatement(reader, partitionHelper.getPartitionKey(writeTime)));
            Assert.assertEquals(0, resultSet.getAvailableWithoutFetching());
        } catch (Exception e) {

        }
    }

    private Statement getSelectQueueStatement(final PhotonBeamReader photonBeamReader,
                                              final Instant partition) {
        return QueryBuilder.select()
                .all()
                .from(BEAM_QUEUE_TABLE)
                .where(eq(BEAM_UUID_COLUMN, photonBeamReader.getBeamUuid()))
                .and(eq(PARTITION_TIME_COLUMN, Timestamp.from(partition)))
                .setConsistencyLevel(CONSISTENCY_LEVEL)
                .setFetchSize(FETCH_SIZE);
    }
}
