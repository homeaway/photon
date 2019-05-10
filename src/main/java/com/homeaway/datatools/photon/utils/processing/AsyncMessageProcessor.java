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

import com.homeaway.datatools.photon.api.beam.Startable;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;

/**
 * An asynchronous event processor designed to process events as they are produced from Photon. This processor
 * preserves order across a provided key of type K and across event keys provided by the ProcessingEvent interface.
 * A ProcessorManifest may be provided in order to track the progress of events as they flow through the processor.
 */
public interface AsyncMessageProcessor<K extends ProcessorKey, V extends ProcessingEvent<T>, T extends Comparable> extends Startable {

    /**
     * Get the total number of events currently present in the processor.
     * @return int - number of events
     */
    int getEventCount();

    /**
     * Get the maximum number of events allowed in the processor at any give time.
     * @return int - the max number of events.
     */
    int getMaxEvents();

    /**
     * Set the maximum number of events allowed in the processor at any give time. Default is
     * 0 which is unlimited.
     *
     * @param maxEvents - the max number of events
     */
    void setMaxEvents(int maxEvents);

    /**
     * Add an event to the processor for processing.
     * @param key - The key provided to preserve order along.
     * @param event - The event to be added to the processor.
     */
    void addEventToQueue(K key, V event);

    /**
     * Is the processor currently actively processing events?
     * @return boolean - whether or not the processor is currently active.
     */
    boolean isActive();

    /**
     * Get the currently state of the processor as a map.
     * @return ConcurrentMap
     */
    ConcurrentMap<String, EventQueueMap<V>> asMap();

    /**
     * Set the interval for the processor to run asynchronously, defaults to 1ms.
     * @param processingLoopInterval - Duration of interval.
     */
    void setProcessingLoopInterval(Duration processingLoopInterval);

    /**
     * Get the interval for the processor to run asynchronously.
     * @return Duration - interval
     */
    Duration getProcessingLoopInterval();

    /**
     * Get the processor manifest if one was provided.
     * @return Optional
     */
    Optional<ProcessorManifest<K, V, T>> getProcessorManifest();
}
