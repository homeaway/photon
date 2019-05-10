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
package com.homeaway.datatools.photon.utils.processing;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DefaultProcessorManifestTest {

    private ConcurrentMap<TestProcessorKey, ConcurrentSkipListMap<Instant, Instant>> processedManifest;
    private ConcurrentMap<String, Manifest<Instant, String>> manifestMap;
    private ProcessorManifest<TestProcessorKey, TestEvent, Instant> processorManifest;

    @Before
    public void init() {
        processedManifest = Maps.newConcurrentMap();
        manifestMap = Maps.newConcurrentMap();
        this.processorManifest = new DefaultProcessorManifest<>(manifestMap, (key, date) -> {
            Instant now = Instant.now();
            processedManifest.computeIfAbsent(key, k -> new ConcurrentSkipListMap<>())
                    .putIfAbsent(now, date);
        });
    }

    @Test
    public void testPutEvent() {
        Instant now = Instant.now();
        List<TestProcessorKey> keys = Lists.newArrayList();
        String BEAM_TEMPLATE = "TestBeamName_%s";
        for(int i = 0; i < 5; i++) {
            keys.add(new TestProcessorKey(String.format(BEAM_TEMPLATE, i)));
        }

        keys.forEach(k -> processorManifest.putEvent(k, new TestEvent(now, "TestKey")));

        Assert.assertEquals(5, manifestMap.size());
    }

    @Test
    public void testManifestMultiThread() {
        ExecutorService executorService = Executors.newFixedThreadPool(100);
        ConcurrentMap<TestProcessorKey, Deque<TestEvent>> events = Maps.newConcurrentMap();
        List<TestProcessorKey> keys = Lists.newArrayList();
        String BEAM_TEMPLATE = "TestBeamName_%s";
        for(int i = 0; i < 5; i++) {
            keys.add(new TestProcessorKey(String.format(BEAM_TEMPLATE, i)));
        }

        for(int i = 0; i < 100; i++) {
            keys.forEach(key -> {
                TestEvent event = new TestEvent(Instant.now(), UUID.randomUUID().toString());
                processorManifest.putEvent(key, event);
                events.computeIfAbsent(key, k -> Lists.newLinkedList()).add(event);
            });
        }

        Assert.assertEquals(5, manifestMap.size());

        while (events.values().stream().anyMatch(q -> !q.isEmpty())) {
            events.keySet().forEach(k -> executorService.execute(() -> {
                TestEvent event = events.get(k).poll();
                if (event != null) {
                    processorManifest.removeEvent(k, event);
                }
            }));
        }

        Assert.assertFalse(processedManifest.isEmpty());

        keys.forEach(key -> Optional.ofNullable(processedManifest.get(key))
                .ifPresent(m -> {
                    Instant prev = null;
                    for (Map.Entry<Instant, Instant> i : m.entrySet()) {
                        if (prev == null) {
                            prev = i.getValue();
                        } else {
                            Assert.assertTrue(i.getValue().isAfter(prev));
                            prev = i.getValue();
                        }
                    }
                }));
    }

    @Data
    private static class TestEvent implements ProcessingEvent<Instant> {

        private final Instant eventDate;
        private final String eventKey;

        public TestEvent(final Instant eventDate,
                         final String eventKey) {
            this.eventDate = eventDate;
            this.eventKey = eventKey;
        }

        @Override
        public String getEventKey() {
            return eventKey;
        }

        @Override
        public Instant getEventOrderingKey() {
            return eventDate;
        }
    }

    @Data
    @AllArgsConstructor
    private static class TestProcessorKey implements ProcessorKey {

        private String beamName;

        @Override
        public String getKeyValue() {
            return beamName;
        }
    }
}