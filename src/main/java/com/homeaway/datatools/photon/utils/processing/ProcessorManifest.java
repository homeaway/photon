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

/**
 * A manifest to track the progress of events as they run through an AsyncMessageProcessor. The data types will
 * be inherited from the AsyncMessageProcessor that the manifest belongs to.
 */
public interface ProcessorManifest<K extends ProcessorKey, V extends ProcessingEvent<T>, T extends Comparable> {

    /**
     * Put an event into the manifest.
     * @param key - The key of the AsyncMessageProcessor.
     * @param event - The event to be added.
     */
    void putEvent(K key, V event);

    /**
     * Remove an event from the manifest. This is call once an event has been processed and ejected from the processor.
     * @param key - The key of the processor.
     * @param event - The event to be ejected.
     */
    void removeEvent(K key, V event);

    /**
     * Eject an entire key from the a manifest and call back to the provided BiConsumer to complete some action indicating
     * the progress of the processor.
     * @param key - The key of the processor.
     */
    void ejectManifest(K key);
}