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
package com.homeaway.datatools.photon.driver;

import com.datastax.driver.core.Cluster;
import static com.datastax.driver.core.HostDistance.LOCAL;
import static com.datastax.driver.core.HostDistance.REMOTE;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import static com.homeaway.datatools.photon.client.PhotonPropertyConstants.PARTITION_SIZE_MILLIS;
import com.homeaway.datatools.photon.dao.beam.BeamDao;
import com.homeaway.datatools.photon.dao.beam.BeamDataDao;
import com.homeaway.datatools.photon.dao.beam.BeamDataManifestDao;
import com.homeaway.datatools.photon.dao.beam.BeamProcessedDao;
import com.homeaway.datatools.photon.dao.beam.BeamReaderDao;
import com.homeaway.datatools.photon.dao.beam.BeamReaderLockDao;
import com.homeaway.datatools.photon.dao.beam.BeamSchemaDao;
import com.homeaway.datatools.photon.dao.beam.cassandra.CassandraBeamDao;
import com.homeaway.datatools.photon.dao.beam.cassandra.CassandraBeamDataDao;
import com.homeaway.datatools.photon.dao.beam.cassandra.CassandraBeamDataManifestDao;
import com.homeaway.datatools.photon.dao.beam.cassandra.CassandraBeamProcessedDao;
import com.homeaway.datatools.photon.dao.beam.cassandra.CassandraBeamReaderDao;
import com.homeaway.datatools.photon.dao.beam.cassandra.CassandraBeamReaderLockDao;
import com.homeaway.datatools.photon.dao.beam.cassandra.CassandraBeamSchemaDao;
import static com.homeaway.datatools.photon.utils.client.ClientConstants.BEAM_READ_LOCK_THRESHOLD;
import static com.homeaway.datatools.photon.utils.client.ClientConstants.PARTITION_SIZE_MILLISECONDS;
import com.homeaway.datatools.photon.utils.dao.DefaultPartitionHelper;
import com.homeaway.datatools.photon.utils.dao.PartitionHelper;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Slf4j
public class CassandraPhotonDriver implements PhotonDriver {

    public static String CLIENT_SSL_OPTIONS = "client.ssl.options";
    public static String SESSION_USER_NAME = "session.username";
    public static String SESSION_PASSWORD = "session.password";
    public static String SESSION_CONTACT_POINTS = "session.contact.points";
    public static String SESSION_KEYSPACE = "session.photon.keyspace";

    public static String MULTI_REGION_SESSION_USER_NAME = "multi.region.session.username";
    public static String MULTI_REGION_SESSION_PASSWORD = "multi.region.session.password";
    public static String MULTI_REGION_SESSION_CONTACT_POINTS = "multi.region.session.contact.points";
    public static String MULTI_REGION_SESSION_KEYSPACE = "multi.region.photon.keyspace";

    private final Properties properties;
    private final PartitionHelper partitionHelper;
    private Session session;
    private Session multiRegionSession;
    private BeamDao beamDao;
    private BeamDataDao beamDataDao;
    private BeamDataManifestDao beamDataManifestDao;
    private BeamProcessedDao beamProcessedDao;
    private BeamReaderDao beamReaderDao;
    private BeamReaderLockDao beamReaderLockDao;
    private BeamSchemaDao beamSchemaDao;


    public CassandraPhotonDriver(final Properties properties) {
        this.properties = properties;
        this.partitionHelper = new DefaultPartitionHelper(Optional.ofNullable(properties.getProperty(PARTITION_SIZE_MILLIS))
                .map(Integer::parseInt).orElse(PARTITION_SIZE_MILLISECONDS));
    }

    @Override
    public void shutDown() {
        Optional.ofNullable(session)
                .filter(s -> !s.isClosed())
                .ifPresent(Session::close);
        Optional.ofNullable(multiRegionSession)
                .filter(s -> !s.isClosed())
                .ifPresent(Session::close);
        beamDao = null;
        beamDataDao = null;
        beamDataManifestDao = null;
        beamProcessedDao = null;
        beamReaderDao = null;
        beamReaderLockDao = null;
        beamSchemaDao = null;
    }

    @Override
    public BeamDao getBeamDao() {
        return Optional.ofNullable(beamDao)
                .orElseGet(() -> {
                    beamDao = new CassandraBeamDao(getSession(properties));
                    return beamDao;
                });
    }

    @Override
    public BeamDataDao getBeamDataDao() {
        return Optional.ofNullable(beamDataDao)
                .orElseGet(() -> {
                    beamDataDao = new CassandraBeamDataDao(getSession(properties), getPartitionHelper(), getBeamDataManifestDao());
                    return beamDataDao;
                });
    }

    @Override
    public BeamDataManifestDao getBeamDataManifestDao() {
        return Optional.ofNullable(beamDataManifestDao)
                .orElseGet(() -> {
                    beamDataManifestDao = new CassandraBeamDataManifestDao(getSession(properties), getPartitionHelper());
                    return beamDataManifestDao;
                });
    }

    @Override
    public BeamProcessedDao getBeamProcessedDao() {
        return Optional.ofNullable(beamProcessedDao)
                .orElseGet(() -> {
                    beamProcessedDao = new CassandraBeamProcessedDao(getMultiRegionSession(properties), getPartitionHelper());
                    return beamProcessedDao;
                });
    }

    @Override
    public BeamReaderDao getBeamReaderDao() {
        return Optional.ofNullable(beamReaderDao)
                .orElseGet(() -> {
                    beamReaderDao = new CassandraBeamReaderDao(getMultiRegionSession(properties));
                    return beamReaderDao;
                });
    }

    @Override
    public BeamReaderLockDao getBeamReaderLockDao() {
        return Optional.ofNullable(beamReaderLockDao)
                .orElseGet(() -> {
                    beamReaderLockDao = new CassandraBeamReaderLockDao(getMultiRegionSession(properties), BEAM_READ_LOCK_THRESHOLD);
                    return beamReaderLockDao;
                });
    }

    @Override
    public BeamSchemaDao getBeamSchemaDao() {
        return Optional.ofNullable(beamSchemaDao)
                .orElseGet(() -> {
                    beamSchemaDao = new CassandraBeamSchemaDao(getSession(properties));
                    return beamSchemaDao;
                });
    }

    @Override
    public PartitionHelper getPartitionHelper() {
        return partitionHelper;
    }

    private Session buildSession(String[] contactPoints, String userName, String password, String keySpace, SSLOptions sslOptions) {
        Cluster.Builder builder = new Cluster.Builder()
                .addContactPoints(contactPoints)
                .withAuthProvider(new PlainTextAuthProvider(userName, password))
                .withLoadBalancingPolicy(getLoadBalancingPolicy())
                .withPoolingOptions(getPoolingOptions())
                .withoutJMXReporting();

        if (sslOptions != null) {
            builder.withSSL(sslOptions);
        }

        try {
            return  builder.build().connect(keySpace);
        } catch (NoHostAvailableException e) {
            log.warn("Failed to connect to the cassandra DB with SSL enabled, trying again without SSL: {}", e.getMessage());
            return new Cluster.Builder()
                    .addContactPoints(contactPoints)
                    .withAuthProvider(new PlainTextAuthProvider(userName, password))
                    .withLoadBalancingPolicy(getLoadBalancingPolicy())
                    .withPoolingOptions(getPoolingOptions())
                    .withoutJMXReporting()
                    .build()
                    .connect(keySpace);
        }

    }

    private Session getSession(final Properties properties) {
        return Optional.ofNullable(session)
                .filter(s -> !s.isClosed())
                .orElseGet(() -> {
                    session = buildSession(properties.getProperty(SESSION_CONTACT_POINTS).split(","), properties.getProperty(SESSION_USER_NAME),
                            properties.getProperty(SESSION_PASSWORD), properties.getProperty(SESSION_KEYSPACE),
                            (SSLOptions) properties.get(CLIENT_SSL_OPTIONS));
            return session;
        });
    }

    private Session getMultiRegionSession(final Properties properties) {
        return Optional.ofNullable(multiRegionSession)
                .filter(s -> !s.isClosed())
                .orElseGet(() -> {
                    multiRegionSession = Optional.ofNullable(properties.getProperty(MULTI_REGION_SESSION_KEYSPACE))
                            .map(k -> buildSession(Optional.ofNullable(properties.getProperty(MULTI_REGION_SESSION_CONTACT_POINTS))
                                            .orElse(properties.getProperty(SESSION_CONTACT_POINTS)).split(","),
                                    Optional.ofNullable(properties.getProperty(MULTI_REGION_SESSION_USER_NAME)).orElse(properties.getProperty(SESSION_USER_NAME)),
                                    Optional.ofNullable(properties.getProperty(MULTI_REGION_SESSION_PASSWORD)).orElse(properties.getProperty(SESSION_PASSWORD)),
                                    Optional.ofNullable(properties.getProperty(MULTI_REGION_SESSION_KEYSPACE)).orElse(properties.getProperty(SESSION_KEYSPACE)),
                                    (SSLOptions) properties.get(CLIENT_SSL_OPTIONS)
                            )).orElse(getSession(properties));
                    return multiRegionSession;
                });
    }

    private static LoadBalancingPolicy getLoadBalancingPolicy() {
        DCAwareRoundRobinPolicy roundRobinPolicy = DCAwareRoundRobinPolicy.builder()
                .build();
        TokenAwarePolicy tokenAwarePolicy = new TokenAwarePolicy(roundRobinPolicy);
        return LatencyAwarePolicy.builder(tokenAwarePolicy)
                .withExclusionThreshold(1.2)
                .withMininumMeasurements(50)
                .withRetryPeriod(5L, TimeUnit.SECONDS)
                .withScale(25, TimeUnit.MILLISECONDS)
                .withUpdateRate(100, TimeUnit.MILLISECONDS)
                .build();
    }

    private static PoolingOptions getPoolingOptions() {
        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions.setCoreConnectionsPerHost(LOCAL, 2);
        poolingOptions.setCoreConnectionsPerHost(REMOTE, 2);
        poolingOptions.setMaxConnectionsPerHost(LOCAL, 10);
        poolingOptions.setMaxConnectionsPerHost(REMOTE, 4);
        poolingOptions.setMaxRequestsPerConnection(LOCAL, 30000);
        poolingOptions.setMaxRequestsPerConnection(REMOTE, 30000);
        return poolingOptions;
    }


}
