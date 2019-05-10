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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import com.google.common.collect.AbstractIterator;

import java.util.Iterator;

public class CqlSplitter implements Iterable<String> {
    private final String cqlSource;

    public CqlSplitter(String cqlSource) {
        this.cqlSource = checkNotNull(cqlSource, "cqlSource");
    }

    @Override
    public Iterator<String> iterator() {
        // Breaks up a CQL statement using the same lexer rules as Cassandra.  See Cassandra Cql.g, especially
        // the lexer rules (the ones that start with capital letters).  Hand-coded lexers are always hard to
        // read, so this implementation favors readability (as much as possible) over performance without
        // actually bringing in a dependency on an external lexer library.
        return new AbstractIterator<String>() {
            private int pos;

            @Override
            protected String computeNext() {
                while (notEof()) {
                    int start = pos;
                    boolean empty = true;
                    while (notEof() && !consume(';')) {
                        boolean ignorable = false;  // true if comment or whitespace
                        if (consume("$$")) {
                            // pg-style string literal.  skip all input until next '$$'
                            while (notEof() && !consume("$$")) {
                                consumeOne();
                            }
                        } else if (consume('\'')) {
                            // regular quoted string literal
                            while (notEof() && !consumeSkipDoubles('\'')) {
                                consumeOne();
                            }
                        } else if (consume('"')) {
                            // quoted name
                            while (notEof() && !consumeSkipDoubles('"')) {
                                consumeOne();
                            }
                        } else if (consume("--") || consume("//")) {
                            // single-line comment (sql or c-style)
                            while (notEof() && !(consume('\n') || consume('\r'))) {
                                consumeOne();
                            }
                            ignorable = true;
                        } else if (consume("/*")) {
                            // multi-line comment
                            while (notEof()) {
                                if (consume("*/")) {
                                    ignorable = true;  // don't ignore the comment if we encounter eof before '*/'
                                    break;
                                }
                                consumeOne();
                            }
                        } else if (consume(' ') || consume('\t') || consume('\n') || consume('\r')) {
                            ignorable = true;
                        } else {
                            consumeOne();
                        }
                        empty &= ignorable;
                    }
                    if (empty) {
                        continue;  // all comments or whitespace
                    }
                    return cqlSource.substring(start, pos);
                }
                return endOfData();
            }

            private boolean notEof() {
                return pos < cqlSource.length();
            }

            private int peekCh() {
                return notEof() ? cqlSource.charAt(pos) : -1;
            }

            /** Unconditionally advances the current position one character. */
            private void consumeOne() {
                checkState(notEof());
                pos++;
            }

            /** Advances the current position one character iff the next char is equal to {@code ch}. */
            private boolean consume(char ch) {
                if (peekCh() == ch) {
                    pos++;
                    return true;
                }
                return false;
            }

            /** Advances the current position past {@code s} iff the next substring is equal to {@code s}. */
            private boolean consume(String s) {
                if (cqlSource.regionMatches(pos, s, 0, s.length())) {
                    pos += s.length();
                    return true;
                }
                return false;
            }

            /**
             * Similar to {@link #consume(char)} in that it consumes the next char iff it is equal to {@code ch},
             * but after advancing the current position it returns true only if the character following is not
             * also {@code ch}.  So there are 3 cases to consider:
             * <ol>
             *     <li>{@code ch} - advances one character and returns {@code true}</li>
             *     <li>{@code ch ch} - advances one character and returns {@code false}, leaving the position
             *                         pointing at the second {@code ch}</li>
             *     <li>{@code &lt;anything else>} - does nothing and returns {@code false}</li>
             * </ol>
             */
            private boolean consumeSkipDoubles(char ch) {
                if (peekCh() == ch) {
                    pos++;
                    return peekCh() != ch;
                }
                return false;
            }
        };
    }
}
