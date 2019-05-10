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
package com.homeaway.datatools.photon.utils.test.embed;

import com.datastax.driver.core.Cluster;
import com.homeaway.datatools.photon.utils.test.cassandra.embed.EmbeddedCassandra;
import static junit.framework.TestCase.assertEquals;
import org.junit.Test;

public class EmbeddedCassandraTest {
    @Test
    public void testStartAndConnect() {
        Cluster cluster = EmbeddedCassandra.startAndConnect();
        assertEquals("Test Cluster", cluster.getMetadata().getClusterName());
    }

    @Test
    public void testStatus() {
        EmbeddedCassandra.start();
        EmbeddedCassandra.Status status = EmbeddedCassandra.getStatus();
        assertEquals(EmbeddedCassandra.Status.STARTED, status);
    }
}
