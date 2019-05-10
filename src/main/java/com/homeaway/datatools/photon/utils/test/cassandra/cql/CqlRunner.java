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
package com.homeaway.datatools.photon.utils.test.cassandra.cql;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.base.Throwables;
import com.google.common.io.Resources;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

@Slf4j
public class CqlRunner implements Closeable {

    private final Session session;
    private final boolean sessionOwner;

    public CqlRunner(Cluster cluster) {
        this(cluster.connect(), true);
    }

    public CqlRunner(Cluster cluster, String keyspace) {
        this(cluster.connect(keyspace), true);
    }

    public CqlRunner(Session session) {
        this(session, false);
    }

    public CqlRunner(Session session, boolean sessionOwner) {
        this.session = session;
        this.sessionOwner = sessionOwner;
    }

    public void executeResource(String resourcePath) {
        String cql;
        try {
            URL schemaUrl = Resources.getResource(resourcePath);
            cql = Resources.toString(schemaUrl, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        execute(cql);
    }

    public void execute(String cql) {
        for (String statement : new CqlSplitter(cql)) {
            log.trace("Executing CQL fragment: {}", statement);
            try {
                session.execute(statement);
            } catch (Exception e) {
                throw new RuntimeException(String.format("Exception executing CQL string:\n%s", statement), e);
            }
        }
    }

    @Override
    public void close() {
        if (sessionOwner) {
            session.close();
        }
    }
}
