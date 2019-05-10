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
import lombok.Data;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

public class DefaultEventQueueMapTest {

    private ConcurrentMap<String, EventQueueMap.EventQueue<TestEvent>> eventQueueMap;
    private EventQueueMap<TestEvent> eventEventQueueMap;

    @Before
    public void init() {
        eventQueueMap = Maps.newConcurrentMap();
        eventEventQueueMap = new DefaultEventQueueMap<>(eventQueueMap, new LongAdder());
    }

    @Test
    public void testPutEvent() {
        UUID key = UUID.randomUUID();
        UUID key2 = UUID.randomUUID();
        Assert.assertTrue(eventQueueMap.isEmpty());
        eventEventQueueMap.putEvent(new TestEvent(Instant.now(), key.toString()));
        Assert.assertFalse(eventQueueMap.isEmpty());
        Assert.assertEquals(1, eventQueueMap.size());
        eventEventQueueMap.putEvent(new TestEvent(Instant.now(), key2.toString()));
        eventEventQueueMap.putEvent(new TestEvent(Instant.now(), key.toString()));
        eventEventQueueMap.putEvent(new TestEvent(Instant.now(), key2.toString()));
        Assert.assertFalse(eventQueueMap.isEmpty());
        Assert.assertEquals(2, eventQueueMap.size());
        Assert.assertEquals(4, eventEventQueueMap.getEventCount());
        Assert.assertFalse(eventEventQueueMap.queueIsEmpty(key.toString()));
        Assert.assertFalse(eventEventQueueMap.queueIsEmpty(key2.toString()));
        Assert.assertTrue(eventEventQueueMap.queueIsEmpty(UUID.randomUUID().toString()));
    }

    @Test
    public void testPeekAndPopQueue() {
        List<UUID> keys = Lists.newArrayList(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        for(int i = 0; i < 100; i++) {
            keys.forEach(k -> eventEventQueueMap.putEvent(new TestEvent(Instant.now(), k.toString())));
        }
        Assert.assertEquals(5, eventQueueMap.size());
        Assert.assertEquals(500, eventEventQueueMap.getEventCount());

        while (eventEventQueueMap.getEventCount() > 0) {
            keys.forEach(k -> {
                int eventCount = eventEventQueueMap.getEventCount();
                Assert.assertNotNull(eventEventQueueMap.peekQueue(k.toString()));
                Assert.assertTrue(eventEventQueueMap.popQueue(k.toString()));
                Assert.assertEquals(eventCount-1, eventEventQueueMap.getEventCount());
                if (eventEventQueueMap.queueIsEmpty(k.toString())) {
                    eventEventQueueMap.removeEmptyQueue(k.toString());
                }
            });
        }
        Assert.assertEquals(0, eventQueueMap.size());
    }

    @Test
    public void testIterateKeys() {
        List<String> iteratedKeys = Lists.newArrayList();
        List<UUID> keys = Lists.newArrayList(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        for(int i = 0; i < 100; i++) {
            keys.forEach(k -> eventEventQueueMap.putEvent(new TestEvent(Instant.now(), k.toString())));
        }

        eventEventQueueMap.iterateKeys(iteratedKeys::add);

        Assert.assertEquals(5, iteratedKeys.size());
    }

    @Data
    private static class TestEvent implements ProcessingEvent {

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

}