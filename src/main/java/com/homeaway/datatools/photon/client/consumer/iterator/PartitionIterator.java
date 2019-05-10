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

import com.homeaway.datatools.photon.dao.rows.PhotonRow;
import static com.homeaway.datatools.photon.utils.dao.Constants.WRITE_TIME_COLUMN;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class PartitionIterator implements MergeIterator<PhotonRow> {

    private final List<StatefulIterator<PhotonRow>> iterators;

    public PartitionIterator(final List<StatefulIterator<PhotonRow>> iterators) {
        this.iterators = iterators;
    }

    @Override
    public Optional<PhotonRow> getNextEntry() {
        return getNextIterator().map(i -> {
            Optional<PhotonRow> row = i.getCurrentValue();
            i.moveNext();
            return row;
        }).orElse(Optional.empty());
    }

    @Override
    public Boolean isExhausted() {
        return iterators.isEmpty();
    }

    @Override
    public int size() {
        return iterators
                .stream()
                .mapToInt(StatefulIterator::size)
                .sum();
    }

    private Optional<StatefulIterator<PhotonRow>> getNextIterator() {
        StatefulIterator<PhotonRow> currentEntry = null;
        Iterator<StatefulIterator<PhotonRow>> iterator = iterators.iterator();
        while (iterator.hasNext()) {
            StatefulIterator<PhotonRow> entry = iterator.next();
            if (currentEntry == null
                    && entry.getCurrentValue().isPresent()) {
                currentEntry = entry;
            } else {
                if (entry.getCurrentValue().isPresent()) {
                    if (entry.getCurrentValue().get().getInstant(WRITE_TIME_COLUMN)
                        .isBefore(currentEntry.getCurrentValue().get().getInstant(WRITE_TIME_COLUMN))) {
                        currentEntry = entry;
                    }
                } else {
                    iterator.remove();
                }
            }
        }
        return Optional.ofNullable(currentEntry);
    }
}
