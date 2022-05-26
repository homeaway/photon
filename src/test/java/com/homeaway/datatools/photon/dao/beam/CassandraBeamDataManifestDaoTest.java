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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.homeaway.datatools.photon.dao.beam.cassandra.CassandraBeamDataManifestDao;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;
import com.homeaway.datatools.photon.dao.rows.PhotonRowSet;
import com.homeaway.datatools.photon.dao.rows.PhotonRowSetFuture;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_DATA_MANIFEST_TABLE;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_DATA_MASTER_MANIFEST_TABLE;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_UUID_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.MANIFEST_TIME_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.MASTER_MANIFEST_TIME_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.PARTITION_TIME_COLUMN;
import com.homeaway.datatools.photon.utils.dao.DefaultPartitionHelper;
import com.homeaway.datatools.photon.utils.dao.PartitionHelper;
import com.homeaway.datatools.photon.utils.test.cassandra.testing.EmbeddedCassandraRule;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CassandraBeamDataManifestDaoTest {

    private Session session;
    private BeamDataManifestDao beamDataManifestDao;
    private PartitionHelper partitionHelper;

    @Before
    public void init() {
        session = embeddedCassandraRule.cluster().connect("photon");
        partitionHelper = new DefaultPartitionHelper(100);
        beamDataManifestDao = new CassandraBeamDataManifestDao(session, partitionHelper);
    }

    @Rule
    public EmbeddedCassandraRule embeddedCassandraRule = new EmbeddedCassandraRule("cassandra/create_keyspace.cql",
            "cassandra/beam_data.cql", "cassandra/beam_data_manifest.cql", "cassandra/beam_data_master_manifest.cql");

    @Test
    public void testPutBeamMasterManifest() {
        Instant now = Instant.now();
        PhotonBeam beam = CassandraBeamDaoTestHelper.buildPhotonBeam();
        beamDataManifestDao.putBeamMasterManifest(beam.getBeamUuid(), now, null).getUninterruptibly();
        ResultSet resultSet = session.execute(getMasterSelectStatement(beam.getBeamUuid(), now));
        Assert.assertEquals(1, resultSet.getAvailableWithoutFetching());
    }

    @Test
    public void testPutBeamDataManifest() {
        Instant now = Instant.now();
        PhotonBeam beam = CassandraBeamDaoTestHelper.buildPhotonBeam();
        beamDataManifestDao.putBeamDataManifest(beam.getBeamUuid(), now, null).getUninterruptibly();
        ResultSet resultSet = session.execute(getSelectStatement(beam.getBeamUuid(), now));
        Assert.assertEquals(1, resultSet.getAvailableWithoutFetching());
    }

    @Test
    public void testPutBeamMasterManifestMultiple() {
        Instant now = Instant.now();
        Instant then = now.plusSeconds(10);
        PhotonBeam beam = CassandraBeamDaoTestHelper.buildPhotonBeam();
        try {
            Futures.successfulAsList(beamDataManifestDao.putBeamMasterManifest(beam.getBeamUuid(), now, null),
                    beamDataManifestDao.putBeamMasterManifest(beam.getBeamUuid(), then, null)).get();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        Set<Instant> partitions = Sets.newHashSet();
        ResultSet resultSet = session.execute(getMasterSelectStatement(beam.getBeamUuid(), now));
        resultSet.forEach(r -> partitions.add(r.getTimestamp(MANIFEST_TIME_COLUMN).toInstant()));
        if (!partitionHelper.getManifestKey(now).equals(partitionHelper.getManifestKey(then))) {
            resultSet = session.execute(getMasterSelectStatement(beam.getBeamUuid(), then));
            resultSet.forEach(r -> partitions.add(r.getTimestamp(MANIFEST_TIME_COLUMN).toInstant()));
        }
        Assert.assertEquals(2, partitions.size());
    }

    @Test
    public void testPutBeamDataManifestMultiple() {
        Instant now = Instant.now();
        Instant then = now.plusMillis(150);
        PhotonBeam beam = CassandraBeamDaoTestHelper.buildPhotonBeam();
        try {
            Futures.successfulAsList(beamDataManifestDao.putBeamDataManifest(beam.getBeamUuid(), now, null),
                    beamDataManifestDao.putBeamDataManifest(beam.getBeamUuid(), then, null)).get();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        Set<Instant> partitions = Sets.newHashSet();
        ResultSet resultSet = session.execute(getSelectStatement(beam.getBeamUuid(), now));
        resultSet.forEach(r -> partitions.add(r.getTimestamp(PARTITION_TIME_COLUMN).toInstant()));
        if (!partitionHelper.getManifestKey(now).equals(partitionHelper.getManifestKey(then))) {
            resultSet = session.execute(getSelectStatement(beam.getBeamUuid(), then));
            resultSet.forEach(r -> partitions.add(r.getTimestamp(PARTITION_TIME_COLUMN).toInstant()));
        }
        Assert.assertEquals(2, partitions.size());
    }

    @Test
    public void testGetBeamMasterManifest() {
        List<PhotonRowSetFuture> resultSetFutureList = Lists.newArrayList();
        Instant now = Instant.now();
        PhotonBeamReader beamReader = CassandraBeamDaoTestHelper.buildPhotonBeamReader();
        for (int i = 0; i < 20; i++) {
            session.execute(getMasterInsertStatement(beamReader.getBeamUuid(), Instant.now().plusSeconds(i * 6)));
        }
        Instant then = now;
        while(then.isBefore(Instant.now()) || then.equals(Instant.now())) {
            resultSetFutureList.add(beamDataManifestDao.getBeamMasterManifest(beamReader.getBeamUuid(), then, now, now.plusSeconds(120)));
            then = then.plus(1L, ChronoUnit.MINUTES);
        }
        try {
            Assert.assertEquals(20, Futures.successfulAsList(resultSetFutureList).get()
                    .stream().mapToInt(PhotonRowSet::getSize).sum());
        } catch (ExecutionException | InterruptedException e) {

        }
    }

    @Test
    public void testGetBeamMasterManifestInRange() {
        List<PhotonRowSetFuture> resultSetFutureList = Lists.newArrayList();
        Instant now = Instant.now();
        PhotonBeamReader beamReader = CassandraBeamDaoTestHelper.buildPhotonBeamReader();
        for (int i = 0; i < 20; i++) {
            session.execute(getMasterInsertStatement(beamReader.getBeamUuid(), Instant.now().plusSeconds(i * 6)));
        }
        Instant then = now;
        while(then.isBefore(Instant.now()) || then.equals(Instant.now())) {
            resultSetFutureList.add(beamDataManifestDao.getBeamMasterManifest(beamReader.getBeamUuid(), then, now, now.plusSeconds(54)));
            then = then.plus(1L, ChronoUnit.MINUTES);
        }
        try {
            Assert.assertEquals(10, Futures.successfulAsList(resultSetFutureList).get()
                    .stream().mapToInt(PhotonRowSet::getSize).sum());
        } catch (ExecutionException | InterruptedException e) {

        }
    }

    @Test
    public void testGetBeamDataManifest() {
        List<PhotonRowSetFuture> resultSetFutureList = Lists.newArrayList();
        Instant now = Instant.now();
        PhotonBeamReader beamReader = CassandraBeamDaoTestHelper.buildPhotonBeamReader();
        for (int i = 0; i < 20; i++) {
            session.execute(getInsertStatement(beamReader.getBeamUuid(), Instant.now()));
        }
        while(now.isBefore(Instant.now()) || now.equals(Instant.now())) {
            resultSetFutureList.add(beamDataManifestDao.getBeamDataManifest(beamReader.getBeamUuid(), now, now));
            now = now.plus(1L, ChronoUnit.MINUTES);
        }
        try {
            List<PhotonRowSet> photonRowSetsFromSuccessfulFutures = Futures.successfulAsList(resultSetFutureList).get();
            final int actualCount = photonRowSetsFromSuccessfulFutures
                    .stream().mapToInt(PhotonRowSet::getSize).sum();
            Assert.assertEquals(
                    String.format(
                            "Expected total of 20 elements in following list, but got %s. The list is: %s",
                            actualCount,
                            photonRowSetsFromSuccessfulFutures.stream()
                                    .flatMap(x -> StreamSupport.stream(x.spliterator(), false))
                                    .map(Object::toString)
                                    .collect(Collectors.joining(",\n"))
                    ),
                    20,
                    actualCount
            );
        } catch (ExecutionException | InterruptedException e) {

        }
    }

    private Statement getMasterInsertStatement(UUID beamUuid, Instant partitionTime) {
        return insertInto(BEAM_DATA_MASTER_MANIFEST_TABLE)
                .value(BEAM_UUID_COLUMN, beamUuid)
                .value(MASTER_MANIFEST_TIME_COLUMN, Timestamp.from(partitionHelper.getMasterManifestKey(partitionTime)))
                .value(MANIFEST_TIME_COLUMN, Timestamp.from(partitionHelper.getManifestKey(partitionTime)));
    }

    private Statement getInsertStatement(UUID beamUuid, Instant partitionTime) {
        return insertInto(BEAM_DATA_MANIFEST_TABLE)
                .value(BEAM_UUID_COLUMN, beamUuid)
                .value(MANIFEST_TIME_COLUMN, Timestamp.from(partitionHelper.getManifestKey(partitionTime)))
                .value(PARTITION_TIME_COLUMN, Timestamp.from(partitionTime));
    }

    private Statement getMasterSelectStatement(UUID beamUuid, Instant partitionTime) {
        return select()
                .all()
                .from(BEAM_DATA_MASTER_MANIFEST_TABLE)
                .where(eq(BEAM_UUID_COLUMN, beamUuid))
                .and(eq(MASTER_MANIFEST_TIME_COLUMN, Timestamp.from(partitionHelper.getMasterManifestKey(partitionTime))));
    }

    private Statement getSelectStatement(UUID beamUuid, Instant partitionTime) {
        return select()
                .all()
                .from(BEAM_DATA_MANIFEST_TABLE)
                .where(eq(BEAM_UUID_COLUMN, beamUuid))
                .and(eq(MANIFEST_TIME_COLUMN, Timestamp.from(partitionHelper.getManifestKey(partitionTime))));
    }

}
