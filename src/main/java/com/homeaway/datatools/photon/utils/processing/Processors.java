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
 * A class that provides methods for instantiating processors.
 */
public class Processors {

    /**
     * Build an AsyncMessageProcessor with an included processor manifest.
     * @param messageEventHandler - The handler that will be called back to for each event.
     * @param processorManifest - The manifest.
     * @param <K> - The data type of the Key
     * @param <V> - The data type of the events.
     * @param <T> - The type of the values that will be used for ordering the manifest.
     * @return - A new AsyncMessageProcessor with a processor manifest.
     */
    public static <K extends ProcessorKey, V extends ProcessingEvent<T>, T extends Comparable> AsyncMessageProcessor<K, V, T> newAsyncMessageProcessor(MessageEventHandler<K, V> messageEventHandler,
                                                                                                                                                       ProcessorManifest<K, V, T> processorManifest) {
        return new DefaultAsyncMessageProcessor<>(messageEventHandler, processorManifest);
    }

    /**
     * Build an AsyncMessageProcessor without an included processor manifest.
     * @param messageEventHandler - The handler that will be called back to for each event.
     * @param <K> - The data type of the Key
     * @param <V> - The data type of the events.
     * @param <T> - The type of the values that will be used for ordering the manifest.
     * @return - A new AsyncMessageProcessor without a processor manifest.
     */
    public static <K extends ProcessorKey, V extends ProcessingEvent<T>, T extends Comparable> AsyncMessageProcessor<K, V, T> newAsyncMessageProcessor(MessageEventHandler<K, V> messageEventHandler) {
        return new DefaultAsyncMessageProcessor<>(messageEventHandler);
    }

}