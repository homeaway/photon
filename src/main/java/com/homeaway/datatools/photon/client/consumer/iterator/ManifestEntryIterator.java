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
package com.homeaway.datatools.photon.client.consumer.iterator;

import com.google.common.collect.Lists;
import com.homeaway.datatools.photon.dao.rows.PhotonRow;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class ManifestEntryIterator implements MergeIterator<ManifestEntry> {

    private final ManifestType manifestType;
    private final Map<UUID, StatefulIterator<PhotonRow>> iterators;

    public ManifestEntryIterator(final ManifestType manifestType,
                                 final Map<UUID, StatefulIterator<PhotonRow>> iterators) {
        this.manifestType = manifestType;
        this.iterators = iterators;
    }

    @Override
    public Optional<ManifestEntry> getNextEntry() {
        List<Map.Entry<UUID, StatefulIterator<PhotonRow>>> nextIterators = getNextIterators();
        if (nextIterators.isEmpty()) {
            return Optional.empty();
        } else {
            ManifestEntry entry = new ManifestEntry(Lists.newLinkedList());
            nextIterators.forEach(e -> {
                if (entry.getPartitionTime() == null) {
                    entry.setPartitionTime(e.getValue().getCurrentValue().get().getInstant(manifestType.getPartitionColumnName()));
                    entry.getBeamUuids().add(e.getKey());
                } else if (entry.getPartitionTime().equals(e.getValue().getCurrentValue().get().getInstant(manifestType.getPartitionColumnName()))) {
                    entry.getBeamUuids().add(e.getKey());
                }
                e.getValue().moveNext();
                if (!e.getValue().getCurrentValue().isPresent()) {
                    iterators.remove(e.getKey());
                }
            });
            return Optional.of(entry);
        }
    }

    @Override
    public Boolean isExhausted() {
        return iterators.isEmpty();
    }

    @Override
    public int size() {
        return iterators.values()
                .stream()
                .mapToInt(StatefulIterator::size)
                .sum();
    }

    private List<Map.Entry<UUID, StatefulIterator<PhotonRow>>> getNextIterators() {
        List<Map.Entry<UUID, StatefulIterator<PhotonRow>>> entries = Lists.newLinkedList();
        Map.Entry<UUID, StatefulIterator<PhotonRow>> currentEntry = null;
        Iterator<Map.Entry<UUID, StatefulIterator<PhotonRow>>> iterator = iterators.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<UUID, StatefulIterator<PhotonRow>> entry = iterator.next();
            if (currentEntry == null
                    && entry.getValue().getCurrentValue().isPresent()) {
                currentEntry = entry;
                entries.add(entry);
            } else {
                if (entry.getValue().getCurrentValue().isPresent()) {
                    if (entry.getValue().getCurrentValue().get().getInstant(manifestType.getPartitionColumnName())
                        .equals(currentEntry.getValue().getCurrentValue().get().getInstant(manifestType.getPartitionColumnName()))) {
                        entries.add(entry);
                    } else if (entry.getValue().getCurrentValue().get().getInstant(manifestType.getPartitionColumnName())
                            .isBefore(currentEntry.getValue().getCurrentValue().get().getInstant(manifestType.getPartitionColumnName()))) {
                        currentEntry = entry;
                        entries.clear();
                        entries.add(entry);
                    }
                } else {
                    iterator.remove();
                }
            }
        }
        return entries;
    }
}
