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
package com.homeaway.datatools.photon.utils.test.cassandra.testing;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.collect.ImmutableList;
import com.homeaway.datatools.photon.utils.test.cassandra.cql.CqlSchemaInitializer;
import com.homeaway.datatools.photon.utils.test.cassandra.cql.SchemaInitializer;
import org.junit.rules.ExternalResource;

public abstract class CassandraRule extends ExternalResource {
    private final SchemaInitializer schemaInitializer;
    private Session session;

    protected CassandraRule() {
        this((SchemaInitializer) null);
    }

    protected CassandraRule(String... schemaResource) {
        this(new CqlSchemaInitializer(ImmutableList.copyOf(schemaResource)));
    }

    protected CassandraRule(SchemaInitializer schemaInitializer) {
        this.schemaInitializer = schemaInitializer;
    }

    protected abstract Cluster buildCluster();

    public Cluster cluster() {
        return session().getCluster();
    }

    public Session session() {
        return checkNotNull(session, "session");
    }

    @Override
    protected void before() throws Throwable {
        session = buildCluster().connect();

        if (schemaInitializer != null) {
            schemaInitializer.initialize(session);
        }
    }

    @Override
    protected void after() {
        session.close();
        session = null;
    }
}
