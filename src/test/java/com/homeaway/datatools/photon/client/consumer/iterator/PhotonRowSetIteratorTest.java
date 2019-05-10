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
import com.homeaway.datatools.photon.dao.rows.PhotonRowSet;
import org.junit.Assert;
import org.junit.Test;
import static org.mockito.Mockito.mock;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

public class PhotonRowSetIteratorTest {

    @Test
    public void test10Rows() {
        List<PhotonRow> totalRows = Lists.newArrayList();
        PhotonRowSet resultSet = new MockPhotonRowSet(getRows(10));
        StatefulIterator<PhotonRow> iterator = new PhotonRowSetIterator(resultSet);
        Optional<PhotonRow> row = iterator.getCurrentValue();
        Assert.assertTrue(row.isPresent());
        while (row.isPresent()) {
            totalRows.add(row.get());
            iterator.moveNext();
            row = iterator.getCurrentValue();
        }
        Assert.assertEquals(10, totalRows.size());
    }

    @Test
    public void test0Rows() {
        PhotonRowSet photonRowSet = new MockPhotonRowSet(getRows(0));
        StatefulIterator<PhotonRow> iterator = new PhotonRowSetIterator(photonRowSet);
        Optional<PhotonRow> row = iterator.getCurrentValue();
        Assert.assertFalse(row.isPresent());
    }

    private class MockPhotonRowSet implements PhotonRowSet {

        private final Queue<PhotonRow> rows;

        public MockPhotonRowSet(final Queue<PhotonRow> rows) {
            this.rows = rows;
        }

        @Override
        public PhotonRow one() {
            return rows.poll();
        }

        @Override
        public int getSize() {
            return 0;
        }

        @Override
        public Iterator<PhotonRow> iterator() {
            return rows.iterator();
        }

    }

    private Queue<PhotonRow> getRows(int numRows) {
        Queue<PhotonRow> rows = Lists.newLinkedList();
        for(int i = 0; i < numRows; i++) {
            PhotonRow row = mock(PhotonRow.class);
            rows.add(row);
        }
        return rows;
    }
}
