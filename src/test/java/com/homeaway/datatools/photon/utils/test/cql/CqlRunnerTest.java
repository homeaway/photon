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
package com.homeaway.datatools.photon.utils.test.cql;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.homeaway.datatools.photon.utils.test.cassandra.cql.CqlRunner;
import com.homeaway.datatools.photon.utils.test.test.LocalCassandra;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import org.junit.Test;

public class CqlRunnerTest {

    @Test
    public void testString() {
        long now = System.currentTimeMillis();
        try (CqlRunner runner = new CqlRunner(LocalCassandra.INSTANCE)) {
            runner.execute(
                    "CREATE KEYSPACE IF NOT EXISTS unit_tests\n" +
                            "    WITH replication = {'class':'SimpleStrategy','replication_factor':1};\n" +
                            "\n" +
                            "CREATE TABLE IF NOT EXISTS unit_tests.cql_runner_string (\n" +
                            "    name ascii PRIMARY KEY,\n" +
                            "    value bigint\n" +
                            ");");
        }
        try (CqlRunner runner = new CqlRunner(LocalCassandra.INSTANCE, "unit_tests")) {
            runner.execute(String.format("INSERT INTO unit_tests.cql_runner_string (name, value) VALUES ('test',%d);", now));
        }
        try (Session session = LocalCassandra.INSTANCE.newSession()) {
            Row row = session.execute("SELECT value FROM unit_tests.cql_runner_string WHERE name = 'test'").one();
            assertNotNull(row);
            assertEquals(now, row.getLong("value"));
        }
    }

    @Test
    public void testResource() {
        String resource = "com/homeaway/datatools/photon/utils/test/cassandra/cql/CqlRunnerResource.cql";
        try (CqlRunner runner = new CqlRunner(LocalCassandra.INSTANCE)) {
            runner.executeResource(resource);
        }
        try (Session session = LocalCassandra.INSTANCE.newSession()) {
            Row row = session.execute("SELECT value FROM unit_tests.cql_runner_resource WHERE name = 'test'").one();
            assertNotNull(row);
            assertEquals(12345678901L, row.getLong("value"));
        }
    }
}