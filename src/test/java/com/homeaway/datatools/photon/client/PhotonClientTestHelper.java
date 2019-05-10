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
package com.homeaway.datatools.photon.client;

import com.google.common.collect.Lists;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;
import com.homeaway.datatools.photon.dao.rows.PhotonRow;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Deque;
import java.util.List;
import java.util.UUID;

public class PhotonClientTestHelper {

    public static PhotonBeam buildPhotonBeam() {
        PhotonBeam beam = new PhotonBeam();
        beam.setBeamUuid(UUID.randomUUID());
        beam.setBeamName("TestBeam");
        beam.setStartDate(Instant.now());
        return beam;
    }

    public static PhotonBeamReader buildPhotonBeamReader() {
        PhotonBeamReader photonBeamReader = new PhotonBeamReader();
        photonBeamReader.setBeamReaderUuid(UUID.randomUUID());
        photonBeamReader.setClientName("TestService");
        photonBeamReader.setBeamUuid(UUID.randomUUID());
        photonBeamReader.setWaterMark(Instant.now());
        return photonBeamReader;
    }

    public static List<Deque<PhotonRow>> generateListOfQueueOfRows(int numQueues, int[] numRows) {
        List<Deque<PhotonRow>> queues = Lists.newArrayList();
        boolean newAdded = true;
        while(newAdded) {
            newAdded = false;
            for (int j = 0; j < numQueues; j++) {
                if (queues.size() - 1 < j) {
                    queues.add(Lists.newLinkedList());
                }
                if (queues.get(j).size() < numRows[j]) {
                    queues.get(j).add(mockRow());
                    newAdded = true;
                }
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return queues;
    }

    private static PhotonRow mockRow() {
        PhotonRow row = mock(PhotonRow.class);
        when(row.getInstant(any(String.class))).thenReturn(Instant.now());
        return row;
    }
}
