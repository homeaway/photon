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

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DefaultAsyncMessageProcessorTest {

    private ConcurrentMap<TestProcessorKey, List<TestEvent>> processedMessages;
    private AsyncMessageProcessor<TestProcessorKey, TestEvent, Instant> processor;

    @Before
    public void init() {
        processedMessages = Maps.newConcurrentMap();
        processor = new DefaultAsyncMessageProcessor<>(new MessageEventHandler<TestProcessorKey, TestEvent>() {

            @Override
            public void handleEvent(TestProcessorKey key, TestEvent event) {
                processedMessages.computeIfAbsent(key, k -> Collections.synchronizedList(Lists.newArrayList())).add(event);
            }

            @Override
            public void handleException(TestProcessorKey key, TestEvent event, MessageProcessorException exception) {

            }
        });
    }

    @Test
    public void testAsyncProcessor() throws Exception {

        List<TestProcessorKey> keys = Lists.newArrayList();
        String BEAM_TEMPLATE = "TestBeamName_%s";
        for(int i = 0; i < 1; i++) {
            keys.add(new TestProcessorKey(String.format(BEAM_TEMPLATE, i)));
        }

        processor.start();
        Assert.assertTrue(processor.isActive());

        for(int i = 0; i < 100; i++) {
            keys.forEach(key -> processor.addEventToQueue(key, new TestEvent(Instant.now(), UUID.randomUUID().toString())));
        }

        do {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        } while (processor.getEventCount() > 0);

        processor.stop();
        Assert.assertFalse(processor.isActive());
        keys.forEach(key -> Assert.assertEquals(100, processedMessages.get(key).size()));
        Assert.assertEquals(0, processor.getEventCount());
    }

    @Test
    public void testProcessingMultipleEventsSameKey() throws Exception {
        List<TestProcessorKey> keys = Lists.newArrayList();
        String BEAM_TEMPLATE = "TestBeamName_%s";

        for(int i = 0; i < 5; i++) {
            keys.add(new TestProcessorKey(String.format(BEAM_TEMPLATE, i)));
        }

        processor.start();
        Assert.assertTrue(processor.isActive());

        for(int i = 0; i < 100; i++) {
            int keyMod = i % 5;
            keys.forEach(key -> processor.addEventToQueue(key, new TestEvent(Instant.now(), String.format("EventKey_%s", keyMod))));
        }

        do {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        } while (processor.getEventCount() > 0);

        processor.stop();
        Assert.assertFalse(processor.isActive());
        keys.forEach(key -> {
            Assert.assertEquals(100, processedMessages.get(key).size());
        });
        Assert.assertEquals(0, processor.getEventCount());
    }

    @Test
    public void testAsyncProcessorCount() throws Exception {
        List<TestProcessorKey> keys = Lists.newArrayList();
        String BEAM_TEMPLATE = "TestBeamName_%s";
        for(int i = 0; i < 5; i++) {
            keys.add(new TestProcessorKey(String.format(BEAM_TEMPLATE, i)));
        }

        for(int i = 0; i < 100; i++) {
            keys.forEach(key -> processor.addEventToQueue(key, new TestEvent(Instant.now(), UUID.randomUUID().toString())));
        }

        Assert.assertEquals(500, processor.getEventCount());

        processor.start();
        Assert.assertTrue(processor.isActive());

        do {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        } while (processor.getEventCount() > 0);

        processor.stop();
        Assert.assertFalse(processor.isActive());
        Assert.assertEquals(0, processor.getEventCount());
    }

    @Test
    public void testProcessingLoopInterval() {

        processor.setProcessingLoopInterval(Duration.ofSeconds(1L));
        Assert.assertEquals(1L, processor.getProcessingLoopInterval().getSeconds());
        processor.setProcessingLoopInterval(Duration.ofSeconds(2L));
        Assert.assertEquals(2L, processor.getProcessingLoopInterval().getSeconds());
    }

    @Test
    public void testGetSetMaxSize() {
        Assert.assertEquals(0, processor.getMaxEvents());
        processor.setMaxEvents(100);
        Assert.assertEquals(100, processor.getMaxEvents());
    }

    @Test
    public void testMexSizeConstraint() throws Exception {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        processor.setMaxEvents(5);
        List<TestProcessorKey> keys = Lists.newArrayList();
        String BEAM_TEMPLATE = "TestBeamName_%s";

        for(int i = 0; i < 5; i++) {
            keys.add(new TestProcessorKey(String.format(BEAM_TEMPLATE, i)));
        }

        keys.forEach(k -> processor.addEventToQueue(k, new TestEvent(Instant.now(), UUID.randomUUID().toString())));

        Assert.assertEquals(5, processor.getEventCount());
        executorService.execute(() -> {
            for(int i = 0; i < 100; i++) {
                keys.forEach(key -> processor.addEventToQueue(key, new TestEvent(Instant.now(), UUID.randomUUID().toString())));
            }
        });
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Assert.assertEquals(5, processor.getEventCount());
        processor.start();

        Assert.assertTrue(processor.isActive());

        do {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        } while (processor.getEventCount() > 0);

        processor.stop();
        Assert.assertFalse(processor.isActive());
        Assert.assertEquals(0, processor.getEventCount());
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
