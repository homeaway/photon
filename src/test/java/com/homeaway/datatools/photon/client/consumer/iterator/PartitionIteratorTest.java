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
import static com.homeaway.datatools.photon.client.PhotonClientTestHelper.generateListOfQueueOfRows;
import com.homeaway.datatools.photon.dao.rows.PhotonRow;
import org.junit.Assert;
import org.junit.Test;

import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class PartitionIteratorTest {

    @Test
    public void test6StatefuleIterators() {
        List<PhotonRow> entries = Lists.newArrayList();
        List<Deque<PhotonRow>> rows = generateListOfQueueOfRows(6, new int[]{10, 10, 10, 10, 10, 10});

        MergeIterator<PhotonRow> partitionIterator = new PartitionIterator(rows.stream()
                .map(TestStatefulIterator::new)
                .collect(Collectors.toList()));

        Optional<PhotonRow> row = partitionIterator.getNextEntry();
        while(row.isPresent()) {
            entries.add(row.get());
            row = partitionIterator.getNextEntry();
        }

        Assert.assertEquals(60, entries.size());
        PhotonRow prevRow = null;
        for(PhotonRow r : entries) {
            if (prevRow == null) {
                prevRow = r;
            } else {
                Assert.assertTrue(prevRow.getInstant("partition_time")
                        .isBefore(r.getInstant("partition_time"))
                        || prevRow.getInstant("partition_time")
                            .equals(r.getInstant("partition_time")));
                prevRow = r;
            }
        }
    }

    @Test
    public void test5StatefuleIterators() {
        List<PhotonRow> entries = Lists.newArrayList();
        List<Deque<PhotonRow>> rows = generateListOfQueueOfRows(5, new int[]{5, 10, 15, 25, 20});

        MergeIterator<PhotonRow> partitionIterator = new PartitionIterator(rows.stream()
                .map(TestStatefulIterator::new)
                .collect(Collectors.toList()));

        Optional<PhotonRow> row = partitionIterator.getNextEntry();
        while(row.isPresent()) {
            entries.add(row.get());
            row = partitionIterator.getNextEntry();
        }

        Assert.assertEquals(75, entries.size());
        PhotonRow prevRow = null;
        for(PhotonRow r : entries) {
            if (prevRow == null) {
                prevRow = r;
            } else {
                Assert.assertTrue(prevRow.getInstant("partition_time")
                        .isBefore(r.getInstant("partition_time"))
                        || prevRow.getInstant("partition_time")
                        .equals(r.getInstant("partition_time")));
                prevRow = r;
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
            int size = rows.size() + getCurrentValue().map(r -> 1).orElse(0);
            return size;
        }
    }
}
