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

import com.google.common.collect.Sets;

import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantLock;

public class DefaultManifest<K extends Comparable<K>, V> implements Manifest<K, V> {

    private final ReentrantLock lock;
    private final SortedMap<K, Set<V>> manifest;
    private volatile K highEjectedKey;
    private volatile K lowEjectedKey;
    private volatile K lastEjectedKey;

    public DefaultManifest() {
        this(new ConcurrentSkipListMap<>());
    }

    public DefaultManifest(final SortedMap<K, Set<V>> manifest) {
        this(manifest, new ReentrantLock());
    }

    public DefaultManifest(final SortedMap<K, Set<V>> manifest,
                           final ReentrantLock lock) {
        this.manifest = manifest;
        this.lock = lock;
    }

    @Override
    public void putEntry(K key, V value) {
        lock.lock();
        try {
            manifest.computeIfAbsent(key, k -> Sets.newConcurrentHashSet()).add(value);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void removeEntry(K key, V value) {
        getManifestEntry(key).ifPresent(m -> m.remove(value));
        lock.lock();
        try {
            if (getManifestEntry(key).map(Set::isEmpty).orElse(false)) {
                removeKey(key);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<K> getEjectionKey() {
        lock.lock();
        try {
            K ejectionKey = highEjectedKey != null && getMapLowestKey().map(lk -> lk.compareTo(highEjectedKey) > 0).orElse(true)
                    ? highEjectedKey
                    : lowEjectedKey;

            if (lastEjectedKey == null) {
                lastEjectedKey = ejectionKey;
                return Optional.ofNullable(lastEjectedKey);
            } else if (ejectionKey != null) {
                if (!lastEjectedKey.equals(ejectionKey)) {
                    lastEjectedKey = ejectionKey;
                    return Optional.of(lastEjectedKey);
                }
            }
            return Optional.empty();

        } finally {
            lock.unlock();
        }
    }

    private void removeKey(K key) {
        if (Optional.ofNullable(manifest.get(key)).map(Set::isEmpty).orElse(false)) {
            manifest.remove(key);

            if (getMapLowestKey().map(lk -> lk.compareTo(key) > 0).orElse(true)) {
                if (lowEjectedKey == null || lowEjectedKey.compareTo(key) < 0) {
                    lowEjectedKey = key;
                }
            } else {
                if (highEjectedKey != null && getMapLowestKey()
                        .map(lk -> lk.compareTo(highEjectedKey) > 0)
                        .orElse(false)) {
                    lowEjectedKey = highEjectedKey;
                } else {
                    if ((lowEjectedKey == null || lowEjectedKey.compareTo(key) < 0)
                            && getMapLowestKey()
                            .map(lk -> lk.compareTo(key) > 0)
                            .orElse(false)) {
                        lowEjectedKey = key;
                    }
                }
            }

            if ((highEjectedKey == null) || highEjectedKey.compareTo(key) < 0) {
                highEjectedKey = key;
            }
        }
    }

    private Optional<K> getMapLowestKey() {
        if (!manifest.isEmpty()) {
            return Optional.of(manifest.firstKey());
        }
        return Optional.empty();
    }

    private Optional<Set<V>> getManifestEntry(K key) {
        return Optional.ofNullable(manifest.get(key));
    }
}
