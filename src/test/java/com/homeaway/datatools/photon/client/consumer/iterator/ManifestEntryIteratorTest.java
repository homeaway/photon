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
import com.google.common.collect.Maps;
import static com.homeaway.datatools.photon.client.PhotonClientTestHelper.generateListOfQueueOfRows;
import static com.homeaway.datatools.photon.client.consumer.iterator.ManifestType.MANIFEST;
import com.homeaway.datatools.photon.dao.rows.PhotonRow;
import static java.util.stream.Collectors.counting;
import org.junit.Assert;
import org.junit.Test;

import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public class ManifestEntryIteratorTest {

    @Test
    public void test4StatefuleIterators() {

        List<ManifestEntry> entries = Lists.newLinkedList();
        UUID beamUuid_1 = UUID.randomUUID();
        UUID beamUuid_2 = UUID.randomUUID();
        UUID beamUuid_3 = UUID.randomUUID();
        UUID beamUuid_4 = UUID.randomUUID();
        Map<UUID, StatefulIterator<PhotonRow>> iterators = Maps.newHashMap();

        List<Deque<PhotonRow>> rows = generateListOfQueueOfRows(4, new int[]{10, 10, 10, 10});

        Long totalRows = rows.stream()
                .flatMap(Deque::stream)
                .collect(Collectors.toList())
                .stream().collect(Collectors.groupingBy(r -> r.getInstant(MANIFEST.getPartitionColumnName()),
                counting()))
                .entrySet().stream().mapToLong(Map.Entry::getValue).count();

        iterators.put(beamUuid_1, new TestStatefulIterator(rows.get(0)));
        iterators.put(beamUuid_2, new TestStatefulIterator(rows.get(1)));
        iterators.put(beamUuid_3, new TestStatefulIterator(rows.get(2)));
        iterators.put(beamUuid_4, new TestStatefulIterator(rows.get(3)));

        MergeIterator<ManifestEntry> manifestEntryIterator = new ManifestEntryIterator(MANIFEST, iterators);
        Optional<ManifestEntry> entry = manifestEntryIterator.getNextEntry();
        if (entry.isPresent()) {
            while (entry.isPresent()) {
                entries.add(entry.get());
                entry = manifestEntryIterator.getNextEntry();
            }
        }
        Assert.assertEquals(totalRows.intValue(), entries.size());
        ManifestEntry prevEntry = null;
        for(ManifestEntry e : entries) {
            if (prevEntry == null) {
                prevEntry = e;
            } else {
                Assert.assertTrue(prevEntry.getPartitionTime().isBefore(e.getPartitionTime()));
                prevEntry = e;
            }
        }
    }

    @Test
    public void test3StatefulIteratorsDifferentSizes() {
        List<ManifestEntry> entries = Lists.newLinkedList();
        UUID beamUuid_1 = UUID.randomUUID();
        UUID beamUuid_2 = UUID.randomUUID();
        UUID beamUuid_3 = UUID.randomUUID();
        Map<UUID, StatefulIterator<PhotonRow>> iterators = Maps.newHashMap();

        List<Deque<PhotonRow>> rows = generateListOfQueueOfRows(3, new int[]{5, 10, 15});

        Long totalRows = rows.stream()
                .flatMap(Deque::stream)
                .collect(Collectors.toList())
                .stream().collect(Collectors.groupingBy(r -> r.getInstant(MANIFEST.getPartitionColumnName()),
                        counting()))
                .entrySet().stream().mapToLong(Map.Entry::getValue).count();

        iterators.put(beamUuid_1, new TestStatefulIterator(rows.get(0)));
        iterators.put(beamUuid_2, new TestStatefulIterator(rows.get(1)));
        iterators.put(beamUuid_3, new TestStatefulIterator(rows.get(2)));
        MergeIterator<ManifestEntry> manifestEntryIterator = new ManifestEntryIterator(MANIFEST, iterators);
        Optional<ManifestEntry> entry = manifestEntryIterator.getNextEntry();
        if (entry.isPresent()) {
            while (entry.isPresent()) {
                entries.add(entry.get());
                entry = manifestEntryIterator.getNextEntry();
            }
        }
        Assert.assertEquals(totalRows.intValue(), entries.size());
        ManifestEntry prevEntry = null;
        for(ManifestEntry e : entries) {
            if (prevEntry == null) {
                prevEntry = e;
            } else {
                Assert.assertTrue(prevEntry.getPartitionTime().isBefore(e.getPartitionTime()));
                prevEntry = e;
            }
        }
    }

    private class TestStatefulIterator implements StatefulIterator<PhotonRow> {

        private final Deque<PhotonRow> rows;
        private PhotonRow currentRow;

        public TestStatefulIterator(final Deque<PhotonRow> rows) {
            this.rows = rows;
            currentRow = rows.poll();
        }

        @Override
        public Optional<PhotonRow> getCurrentValue() {
            return Optional.ofNullable(currentRow);
        }

        @Override
        public void moveNext() {
            currentRow = rows.poll();
        }

        @Override
        public int size() {
            return rows.size();
        }
    }

}
