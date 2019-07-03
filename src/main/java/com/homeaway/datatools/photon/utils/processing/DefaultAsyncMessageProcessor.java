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
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class DefaultAsyncMessageProcessor<K extends ProcessorKey, V extends ProcessingEvent<T>, T extends Comparable> implements AsyncMessageProcessor<K, V, T> {

    private final Lock lock = new ReentrantLock();
    private final Condition fullCondition = lock.newCondition();
    private final ExecutorService executorService;
    private final ScheduledExecutorService scheduledExecutorService;
    private final ConcurrentMap<String, EventQueueMap<V>> eventQueueMapMap;
    private final ConcurrentMap<String, K> eventQueueKeyMap;
    private final MessageEventHandler<K, V> eventHandler;
    private final ProcessorManifest<K, V, T> processorManifest;
    private final LongAdder count;
    private volatile Duration processingLoopInterval;
    private volatile int maxEvents;
    private ScheduledFuture<?> scheduledFuture;

    public DefaultAsyncMessageProcessor(final MessageEventHandler<K, V> eventHandler) {
        this(eventHandler, null);
    }

    public DefaultAsyncMessageProcessor(final MessageEventHandler<K, V> eventHandler,
                                        int maxEvents) {
        this(eventHandler, null, maxEvents);
    }

    public DefaultAsyncMessageProcessor(final MessageEventHandler<K, V> eventHandler,
                                        final ProcessorManifest<K, V, T> processorManifest) {
        this(eventHandler, processorManifest, 0);
    }

    public DefaultAsyncMessageProcessor(final MessageEventHandler<K, V> eventHandler,
                                        final ProcessorManifest<K, V, T> processorManifest,
                                        int maxEvents) {
        this(Executors.newFixedThreadPool(150), Executors.newScheduledThreadPool(5), Maps.newConcurrentMap(), Maps.newConcurrentMap(),
                eventHandler, processorManifest, Duration.ofMillis(1), maxEvents);
    }

    public DefaultAsyncMessageProcessor(final ExecutorService executorService,
                                        final ScheduledExecutorService scheduledExecutorService,
                                        final ConcurrentMap<String, EventQueueMap<V>> eventQueueMapMap,
                                        final ConcurrentMap<String, K> eventQueueKeyMap,
                                        final MessageEventHandler<K, V> eventHandler,
                                        final ProcessorManifest<K, V, T> processorManifest,
                                        Duration processingLoopInterval,
                                        int maxEvents) {
        this.executorService = executorService;
        this.scheduledExecutorService = scheduledExecutorService;
        this.eventQueueMapMap = eventQueueMapMap;
        this.eventQueueKeyMap = eventQueueKeyMap;
        this.eventHandler = eventHandler;
        this.processorManifest = processorManifest;
        this.processingLoopInterval = processingLoopInterval;
        this.maxEvents = maxEvents;
        this.count = new LongAdder();
    }

    @Override
    public int getEventCount() {
        return count.intValue();
    }

    @Override
    public int getMaxEvents() {
        return maxEvents;
    }

    @Override
    public void setMaxEvents(int maxEvents) {
        this.maxEvents = maxEvents;
    }

    @Override
    public void addEventToQueue(K key, V event) {

        lock.lock();
        try {
            while (maxEvents > 0 && getEventCount() >= maxEvents) {
                try {
                    fullCondition.await(5L, MILLISECONDS);
                } catch (InterruptedException e) {
                    log.error("Thread interrupted while adding event {} for key {}", event, key);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
        getProcessorManifest().ifPresent(m -> m.putEvent(key, event));
        getEventQueueMap(key).putEvent(event);
    }

    @Override
    public boolean isActive() {
        return Optional.ofNullable(scheduledFuture).map(this::isActive).orElse(false);
    }

    @Override
    public ConcurrentMap<String, EventQueueMap<V>> asMap() {
        return eventQueueMapMap;
    }

    @Override
    public void setProcessingLoopInterval(Duration processingLoopInterval) {
        try {
            stop();
            this.processingLoopInterval = processingLoopInterval;
            start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Duration getProcessingLoopInterval() {
        return processingLoopInterval;
    }

    @Override
    public void start() throws Exception {
        if (!isActive()) {
            scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(() -> eventQueueMapMap.entrySet()
                    .forEach(e -> e.getValue()
                            .iterateKeys(k -> executorService.execute(() -> {
                                        if (e.getValue().tryQueueLock(k)) {
                                            try {
                                                while (!e.getValue().queueIsEmpty(k)) {
                                                    V event = e.getValue().peekQueue(k);
                                                    try {
                                                        eventHandler.handleEvent(eventQueueKeyMap.get(e.getKey()), event);
                                                        if (!e.getValue().popQueue(k)) {
                                                            log.warn("EventQueue removed while event was being processed." +
                                                                    "Key={}, EventQueueMap={}, Event={}", k, e.getValue(), event);
                                                        }
                                                        getProcessorManifest().ifPresent(m -> m.removeEvent(eventQueueKeyMap.get(e.getKey()), event));
                                                    } catch (Exception ex) {
                                                        eventHandler.handleException(eventQueueKeyMap.get(e.getKey()), event, new MessageProcessorException(ex));
                                                    }
                                                }
                                                e.getValue().removeEmptyQueue(k);
                                            } finally {
                                                e.getValue().releaseQueueLock(k);
                                                lock.lock();
                                                try {
                                                    fullCondition.signalAll();
                                                } finally {
                                                    lock.unlock();
                                                }
                                            }
                                        }
                                    })
                            )
                    ), 0, processingLoopInterval.toMillis(), MILLISECONDS);
        }
    }

    @Override
    public void stop() throws Exception {
        if (isActive()) {
            scheduledFuture.cancel(true);
        }
    }

    @Override
    public void shutdown() throws Exception {
        scheduledExecutorService.shutdown();
        executorService.shutdown();
    }

    @Override
    public Optional<ProcessorManifest<K, V, T>> getProcessorManifest() {
        return Optional.ofNullable(processorManifest);
    }

    private EventQueueMap<V> getEventQueueMap(K key) {
        eventQueueKeyMap.putIfAbsent(key.getKeyValue(), key);
        return eventQueueMapMap.computeIfAbsent(key.getKeyValue(),
                q -> new DefaultEventQueueMap<>(count));
    }

    private boolean isActive(ScheduledFuture<?> scheduledFuture) {
        return !(scheduledFuture.isCancelled() || scheduledFuture.isDone());
    }
}