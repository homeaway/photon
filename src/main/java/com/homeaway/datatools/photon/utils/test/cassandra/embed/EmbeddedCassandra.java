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
package com.homeaway.datatools.photon.utils.test.cassandra.embed;

import com.datastax.driver.core.AtomicMonotonicTimestampGenerator;
import com.datastax.driver.core.Cluster;
import static com.google.common.base.Preconditions.checkState;
import com.google.common.io.Resources;
import com.google.common.net.HostAndPort;
import lombok.extern.slf4j.Slf4j;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import java.io.IOException;

@Slf4j
public class EmbeddedCassandra {

    public enum Status {
        INITIAL,
        STARTED,
        FAILED,
    }

    private static final String CASSANDRA_YAML = "test/cassandra/embed/cassandra.yaml";

    private static Status status = Status.INITIAL;

    /* Starts a Cassandra server inside the current JVM and returns a client session. */
    public static Cluster startAndConnect() {
        start();
        return connect();
    }

    public static synchronized Status getStatus() {
        return status;
    }

    /**
     * Starts a Cassandra server inside the current JVM. Returns once Cassandra is ready to accept client requests.
     * <p>
     * This method is thread-safe and idempotent--if called more than once, subsequent calls after the first will
     * no-op.
     */
    public static synchronized void start() {
        switch (status) {
            case STARTED:
                log.debug("Skipping start of embedded Cassandra server, already started.");
                return;
            case FAILED:
                throw new IllegalStateException("Embedded Cassandra server startup failed, not retrying.");
        }

        log.info("Starting embedded Cassandra server.");
        status = Status.FAILED;

        // Override port settings so they won't conflict with an existing Cassandra daemon running on standard ports.
        setSystemProperty("cassandra.jmx.local.port", "17199");
        setSystemProperty("mx4jport", "19080");

        // Use a custom copy of cassandra.yaml with ports re-mapped to avoid conflicts with an existing Cassandra instance.
        setSystemProperty("cassandra.config", Resources.getResource(CASSANDRA_YAML).toString());
        setSystemProperty("cassandra.storagedir", "target/cassandra");

        // Note there is no clean way to stop and restart Cassandra within a JVM.
        try {
            new EmbeddedCassandraService().start();
        } catch (IOException e) {
            throw new RuntimeException("Unable to start embedded Cassandra for unit tests.", e);
        }

        // Suppress Cassandra shutdown errors (remove once https://issues.apache.org/jira/browse/CASSANDRA-8220 is resolved)
        StorageService.instance.removeShutdownHook();

        status = Status.STARTED;
    }

    /* Sets a system property but only if it has not been set already. */
    private static void setSystemProperty(String key, String value) {
        if (System.getProperty(key) == null) {
            System.setProperty(key, value);
        }
    }

    /*
     * Connects to the local in-memory Cassandra server.  The caller must call {@link Cluster#close()} to avoid
     * leaking resources.
     */
    public static Cluster connect() {
        HostAndPort hostAndPort = getNativeHostAndPort();
        log.debug("Connecting to local Cassandra server at {}...", hostAndPort);
        return Cluster.builder()
                .addContactPoint(hostAndPort.getHostText())
                .withPort(hostAndPort.getPort())
                .withTimestampGenerator(new AtomicMonotonicTimestampGenerator())
                .withoutMetrics()
                .withoutJMXReporting()
                .build();
    }

    /* Returns the host and port on which the local in-memory Cassandra server is listing for CQL traffic. */
    public static HostAndPort getNativeHostAndPort() {
        checkState(getStatus() == Status.STARTED, "Embedded Cassandra has not been started");
        String host = FBUtilities.getBroadcastAddress().getHostAddress();
        int port = DatabaseDescriptor.getNativeTransportPort();
        return HostAndPort.fromParts(host, port);
    }
}
