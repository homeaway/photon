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

import com.google.common.collect.Maps;
import static java.lang.Boolean.FALSE;
import lombok.ToString;

import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

@ToString
public class DefaultEventQueueMap<T extends ProcessingEvent> implements EventQueueMap<T> {

    private final ConcurrentMap<String, EventQueue<T>> eventQueueMap;
    private final ReentrantLock lock;
    private final LongAdder count;

    public DefaultEventQueueMap(final LongAdder count) {
        this(Maps.newConcurrentMap(), count);
    }

    public DefaultEventQueueMap(final ConcurrentMap<String, EventQueue<T>> eventQueueMap,
                                final LongAdder count) {
        this(eventQueueMap, count, new ReentrantLock());
    }

    public DefaultEventQueueMap(final ConcurrentMap<String, EventQueue<T>> eventQueueMap,
                                final LongAdder count,
                                final ReentrantLock lock) {
        this.eventQueueMap = eventQueueMap;
        this.count = count;
        this.lock = lock;
    }

    @Override
    public int getEventCount() {
        return eventQueueMap.values()
                .stream()
                .mapToInt(Queue::size)
                .sum();
    }

    @Override
    public boolean queueIsEmpty(String key) {
        return eventQueueMap.getOrDefault(key, new EventQueue<>()).isEmpty();
    }

    @Override
    public void putEvent(T event) {
        lock.lock();
        try {
            eventQueueMap.computeIfAbsent(event.getEventKey(), k -> new EventQueue<>()).add(event);
            count.increment();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean tryQueueLock(String key) {
        return Optional.ofNullable(eventQueueMap.get(key)).map(EventQueue::tryLock).orElse(false);
    }

    @Override
    public void releaseQueueLock(String key) {
        Optional.ofNullable(eventQueueMap.get(key)).ifPresent(EventQueue::releaseLock);
    }

    @Override
    public Boolean popQueue(String key) {
        return Optional.ofNullable(eventQueueMap.get(key))
                .map(eq -> Optional.ofNullable(eq.poll())
                        .map(e -> {
                            count.decrement();
                            return e;
                        }).isPresent())
                .orElse(FALSE);
    }

    @Override
    public T peekQueue(String key) {
        return eventQueueMap.get(key).peek();
    }

    @Override
    public void removeEmptyQueue(String key) {
        lock.lock();
        try {
            if (getEventQueue(key).map(EventQueue::isEmpty).orElse(false)) {
                Optional.ofNullable(eventQueueMap.remove(key)).ifPresent(EventQueue::releaseLock);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void iterateKeys(Consumer<String> consumer) {
        eventQueueMap.keySet()
                .forEach(consumer);
    }

    private Optional<EventQueue<T>> getEventQueue(String key) {
        return Optional.ofNullable(eventQueueMap.get(key));
    }
}