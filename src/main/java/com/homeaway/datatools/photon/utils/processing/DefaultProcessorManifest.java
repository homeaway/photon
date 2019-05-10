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

import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

public class DefaultProcessorManifest<K extends ProcessorKey, V extends ProcessingEvent<T>, T extends Comparable> implements ProcessorManifest<K, V, T> {

    private final ReentrantLock lock;
    private final ConcurrentMap<String, Manifest<T, String>> manifestMap;
    private final BiConsumer<K, T> manifestPostConsumer;

    public DefaultProcessorManifest() {
        this(null);
    }

    public DefaultProcessorManifest(final BiConsumer<K, T> manifestPostConsumer) {
        this(Maps.newConcurrentMap(), manifestPostConsumer);
    }

    public DefaultProcessorManifest(final ConcurrentMap<String, Manifest<T, String>> manifestMap,
                                    final BiConsumer<K, T> manifestPostConsumer) {
        this.manifestMap = manifestMap;
        this.manifestPostConsumer = manifestPostConsumer;
        this.lock = new ReentrantLock();
    }

    @Override
    public void putEvent(K key, V event) {
        getManifest(key).putEntry(event.getEventOrderingKey(), event.getEventKey());
    }

    @Override
    public void removeEvent(K key, V event) {
        getManifest(key).removeEntry(event.getEventOrderingKey(), event.getEventKey());
        ejectManifest(key);
    }

    @Override
    public void ejectManifest(K key) {
        try {
            lock.lock();
            getManifest(key).getEjectionKey()
                    .ifPresent(k -> getManifestPostConsumer().ifPresent(c -> c.accept(key, k)));
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    private Manifest<T, String> getManifest(K key) {
        return manifestMap.computeIfAbsent(key.getKeyValue(), k -> new DefaultManifest<>());
    }

    private Optional<BiConsumer<K, T>> getManifestPostConsumer() {
        return Optional.ofNullable(manifestPostConsumer);
    }
}