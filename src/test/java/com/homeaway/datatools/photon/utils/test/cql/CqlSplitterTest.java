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

import com.google.common.collect.ImmutableList;
import com.homeaway.datatools.photon.utils.test.cassandra.cql.CqlSplitter;
import static junit.framework.TestCase.assertEquals;
import org.junit.Test;

public class CqlSplitterTest {

    @Test
    public void testEmpty() {
        assertEquals(
                ImmutableList.of(),
                ImmutableList.copyOf(new CqlSplitter("")));
    }

    @Test
    public void testHidden() {
        assertEquals(
                ImmutableList.of(),
                ImmutableList.copyOf(new CqlSplitter("\n\n/*ignore*/\n//comment\n; ; ;--comment\n")));
    }

    @Test
    public void testPgStyleString() {
        assertEquals(
                ImmutableList.of("$$;$;$$;", "foo"),
                ImmutableList.copyOf(new CqlSplitter("$$;$;$$;foo")));
    }

    @Test
    public void testQuotedString() {
        assertEquals(
                ImmutableList.of("';\\';", "''';''';", "foo"),
                ImmutableList.copyOf(new CqlSplitter("';\\';''';''';foo")));
    }

    @Test
    public void testQuotedName() {
        assertEquals(
                ImmutableList.of("\";\\\";", "\"\"\";\"\"\";", "foo"),
                ImmutableList.copyOf(new CqlSplitter("\";\\\";\"\"\";\"\"\";foo")));
    }

    @Test
    public void testUnterminatedSqlComment() {
        assertEquals(
                ImmutableList.of(),
                ImmutableList.copyOf(new CqlSplitter("-- foo")));
    }

    @Test
    public void testUnterminatedJavaComment() {
        assertEquals(
                ImmutableList.of(),
                ImmutableList.copyOf(new CqlSplitter("// foo")));
    }

    @Test
    public void testUnterminatedMultilineComment() {
        assertEquals(
                ImmutableList.of("/* foo"),
                ImmutableList.copyOf(new CqlSplitter("/* foo")));
    }

    @Test
    public void testNestedMultilineComment() {
        // multi-line comments don't nest
        assertEquals(
                ImmutableList.of("\nend*/"),
                ImmutableList.copyOf(new CqlSplitter("/* begin\n/*middle*/;\nend*/")));
    }

    @Test
    public void testSingle() {
        assertEquals(
                ImmutableList.of(
                        "CREATE KEYSPACE keyspace1 WITH replication = {'class':'SimpleStrategy',';':1};"),
                ImmutableList.copyOf(new CqlSplitter("" +
                        "CREATE KEYSPACE keyspace1 WITH replication = {'class':'SimpleStrategy',';':1};\n")));

    }

    @Test
    public void testMultiple() {
        assertEquals(
                ImmutableList.of(
                        "// header comment\n" +
                                "CREATE KEYSPACE keyspace1 WITH replication = {'class':'SimpleStrategy',';':1};", "\n" +
                                "-- sql comment\n" +
                                "CREATE KEYSPACE keyspace2 WITH replication = {'class':'SimpleStrategy',';':3};"),
                ImmutableList.copyOf(new CqlSplitter(
                        "// header comment\n" +
                                "CREATE KEYSPACE keyspace1 WITH replication = {'class':'SimpleStrategy',';':1};\n" +
                                "-- sql comment\n" +
                                "CREATE KEYSPACE keyspace2 WITH replication = {'class':'SimpleStrategy',';':3};  // comment\n")));

    }

    @Test
    public void testSyntaxError() {
        assertEquals(
                ImmutableList.of("bunch of junk ';' \";\" that should be passed through 'unterminated"),
                ImmutableList.copyOf(new CqlSplitter("bunch of junk ';' \";\" that should be passed through 'unterminated")));
    }
}
