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
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.homeaway.datatools.photon.dao.beam.WriteFuture;
import com.homeaway.datatools.photon.dao.rows.PhotonRow;
import com.homeaway.datatools.photon.dao.rows.PhotonRowSet;
import com.homeaway.datatools.photon.dao.rows.PhotonRowSetFuture;
import com.homeaway.datatools.photon.utils.dao.PartitionHelper;
import com.homeaway.datatools.photon.utils.dao.mapper.DefaultEntityMapper;
import com.homeaway.datatools.photon.utils.dao.mapper.EntityMapper;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Date;
import java.util.Iterator;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public abstract class AbstractCassandraDao {

    private static final ConsistencyLevel CONSISTENCY_LEVEL = ConsistencyLevel.LOCAL_QUORUM;
    private final Session session;
    private final EntityMapper entityMapper;
    private final PartitionHelper partitionHelper;

    protected AbstractCassandraDao(final Session session) {
        this(session, null);
    }

    protected AbstractCassandraDao(final Session session,
                                final PartitionHelper partitionHelper) {
        this(session, new DefaultEntityMapper(), partitionHelper);
    }

    protected AbstractCassandraDao(final Session session,
                         final EntityMapper entityMapper,
                         final PartitionHelper partitionHelper) {
        this.session = session;
        this.entityMapper = entityMapper;
        this.partitionHelper = partitionHelper;
    }

    protected  <T> T getMappedEntity(PhotonRow row, Class<T> clazz) {
        try {
            return entityMapper.getMappedEntity(row, clazz);
        } catch (ClassNotFoundException e) {
            log.error("Class not found for {}", clazz.getName());
        }
        return null;
    }

    protected PartitionHelper getPartitionHelper() {
        return partitionHelper;
    }

    protected ResultSet executeQuery(BoundStatement statement) {
        return session.execute(statement.setConsistencyLevel(CONSISTENCY_LEVEL));
    }

    protected ResultSetFuture executeQueryAsync(BoundStatement statement) {
        return session.executeAsync(statement.setConsistencyLevel(CONSISTENCY_LEVEL));
    }

    protected PreparedStatement createPreparedStatement(RegularStatement statement) {
        return session.prepare(statement);
    }

    protected CassandraWriteFuture getWriteFuture(final ResultSetFuture resultSetFuture) {
        return new CassandraWriteFuture(resultSetFuture);
    }

    protected PhotonRowSetFuture getRowSetFuture(final ResultSetFuture resultSetFuture) {
        return new CassandraRowSetFuture(resultSetFuture);
    }

    protected PhotonRowSet getRowSet(final ResultSet resultSet) {
        return new CassandraRowSet(resultSet);
    }

    private static final class CassandraWriteFuture implements WriteFuture {

        private final ResultSetFuture resultSetFuture;

        public CassandraWriteFuture(final ResultSetFuture resultSetFuture) {
            this.resultSetFuture = resultSetFuture;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return resultSetFuture.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return resultSetFuture.isCancelled();
        }

        @Override
        public boolean isDone() {
            return resultSetFuture.isDone();
        }

        @Override
        public Boolean get() throws InterruptedException, ExecutionException {
            return resultSetFuture.get().wasApplied();
        }

        @Override
        public Boolean get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return resultSetFuture.get(timeout, unit).wasApplied();
        }

        @Override
        public Boolean getUninterruptibly() {
            return resultSetFuture.getUninterruptibly().wasApplied();
        }

        @Override
        public Boolean getUninterruptibly(long timeout, TimeUnit unit) throws TimeoutException {
            return resultSetFuture.getUninterruptibly(timeout, unit).wasApplied();
        }

        @Override
        public void addListener(Runnable listener, Executor executor) {
            resultSetFuture.addListener(listener, executor);
        }
    }

    private static class CassandraRowSetFuture implements PhotonRowSetFuture {

        private final ResultSetFuture resultSetFuture;

        public CassandraRowSetFuture(final ResultSetFuture resultSetFuture) {
            this.resultSetFuture = resultSetFuture;
        }

        @Override
        public PhotonRowSet getUninterruptibly() {
            return new CassandraRowSet(resultSetFuture.getUninterruptibly());
        }

        @Override
        public PhotonRowSet getUninterruptibly(long timeout, TimeUnit unit) throws TimeoutException {
            return new CassandraRowSet(resultSetFuture.getUninterruptibly(timeout, unit));
        }

        @Override
        public void addListener(Runnable listener, Executor executor) {
            resultSetFuture.addListener(listener, executor);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return resultSetFuture.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return resultSetFuture.isCancelled();
        }

        @Override
        public boolean isDone() {
            return resultSetFuture.isDone();
        }

        @Override
        public PhotonRowSet get() throws InterruptedException, ExecutionException {
            return new CassandraRowSet(resultSetFuture.get());
        }

        @Override
        public PhotonRowSet get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return new CassandraRowSet(resultSetFuture.get(timeout, unit));
        }
    }

    private static final class CassandraRowSet implements PhotonRowSet {

        private final ResultSet resultSet;

        public CassandraRowSet(final ResultSet resultSet) {
            this.resultSet = resultSet;
        }

        @Override
        public PhotonRow one() {
            return Optional.ofNullable(resultSet.one()).map(CassandraRow::new).orElse(null);
        }

        @Override
        public int getSize() {
            return resultSet.getAvailableWithoutFetching();
        }

        @Override
        public Iterator<PhotonRow> iterator() {
            return new CassandraRowIterator(resultSet.iterator());
        }

        private static final class CassandraRowIterator implements Iterator<PhotonRow> {

            private final Iterator<Row> rowIterator;

            public CassandraRowIterator(final Iterator<Row> rowIterator) {
                this.rowIterator = rowIterator;
            }

            @Override
            public boolean hasNext() {
                return rowIterator.hasNext();
            }

            @Override
            public PhotonRow next() {
                return new CassandraRow(rowIterator.next());
            }
        }
    }

    private static final class CassandraRow implements PhotonRow {

        private final Row row;

        public CassandraRow(final Row row) {
            this.row = row;
        }

        @Override
        public String getString(String columnName) {
            return row.getString(columnName);
        }

        @Override
        public Instant getInstant(String columnName) {
            return Optional.ofNullable(row.getTimestamp(columnName))
                    .map(Date::toInstant)
                    .orElse(null);
        }

        @Override
        public UUID getUuid(String columnName) {
            return row.getUUID(columnName);
        }

        @Override
        public Integer getInt(String columnName) {
            return Optional.ofNullable(row.get(columnName, Integer.class))
                    .orElse(null);
        }

        @Override
        public byte[] getBytes(String columnName) {
            return Optional.ofNullable(row.getBytes(columnName)).map(ByteBuffer::array).orElse(null);
        }
    }
}
