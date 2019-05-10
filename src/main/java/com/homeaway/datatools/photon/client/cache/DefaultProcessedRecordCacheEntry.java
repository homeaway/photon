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
package com.homeaway.datatools.photon.client.cache;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultProcessedRecordCacheEntry implements ProcessedRecordCacheEntry {

    private final ConcurrentMap<Instant, Set<String>> processedRecords;
    private final AtomicInteger size;

    public DefaultProcessedRecordCacheEntry() {
        this(Maps.newConcurrentMap(), new AtomicInteger());
    }

    public DefaultProcessedRecordCacheEntry(final ConcurrentMap<Instant, Set<String>> processedRecords,
                                            final AtomicInteger size) {
        this.processedRecords = processedRecords;
        this.size = size;
    }

    @Override
    public void putValue(Instant writTime, String messageKey) {
        if (processedRecords.computeIfAbsent(writTime, k -> Sets.newConcurrentHashSet()).add(messageKey)) {
            size.incrementAndGet();
        }
    }

    @Override
    public boolean isPresent(Instant writTime, String messageKey) {
        return processedRecords.getOrDefault(writTime, Sets.newConcurrentHashSet()).contains(messageKey);
    }

    @Override
    public int size() {
        return size.get();
    }
}
