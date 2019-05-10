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
package com.homeaway.datatools.photon.utils.client;

import static com.homeaway.datatools.photon.utils.dao.Constants.MASTER_MANIFEST_PARTITION_SIZE;

import java.time.Instant;
import java.util.function.BiFunction;
import java.util.function.Function;

public enum PartitionProducerType {

    FORWARD((start, end) -> start.isBefore(end) || start.equals(end),
            (start) -> start.plusMillis(MASTER_MANIFEST_PARTITION_SIZE.toMillis())),
    REVERSE((start, end) -> start.isAfter(end) || start.equals(end),
            (start) -> start.minusMillis(MASTER_MANIFEST_PARTITION_SIZE.toMillis()));

    private final BiFunction<Instant, Instant, Boolean> manifestPartitionKeyComparator;
    private final Function<Instant, Instant> manifestPartitionKeyMover;

    PartitionProducerType(final BiFunction<Instant, Instant, Boolean> manifestPartitionKeyComparator,
                          final Function<Instant, Instant> manifestPartitionKeyMover) {
        this.manifestPartitionKeyComparator = manifestPartitionKeyComparator;
        this.manifestPartitionKeyMover = manifestPartitionKeyMover;
    }

    public BiFunction<Instant, Instant, Boolean> getManifestPartitionKeyComparator() {
        return manifestPartitionKeyComparator;
    }

    public Function<Instant, Instant> getManifestPartitionKeyMover() {
        return manifestPartitionKeyMover;
    }
}
