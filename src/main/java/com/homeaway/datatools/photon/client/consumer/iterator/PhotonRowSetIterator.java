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
import com.homeaway.datatools.photon.dao.rows.PhotonRowSet;

import java.util.Optional;

public class PhotonRowSetIterator implements StatefulIterator<PhotonRow> {

    private final PhotonRowSet rowSet;
    private PhotonRow currentRow;

    public PhotonRowSetIterator(final PhotonRowSet rowSet) {
        this.rowSet = rowSet;
        this.currentRow = rowSet.one();
    }

    @Override
    public Optional<PhotonRow> getCurrentValue() {
        return Optional.ofNullable(currentRow);
    }

    @Override
    public void moveNext() {
        currentRow = rowSet.one();
    }

    @Override
    public int size() {
        return rowSet.getSize() + getCurrentValue().map(r -> 1).orElse(0);
    }

}
