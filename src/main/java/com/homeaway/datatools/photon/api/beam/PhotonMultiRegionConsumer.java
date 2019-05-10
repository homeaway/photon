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
package com.homeaway.datatools.photon.api.beam;

import com.homeaway.datatools.photon.api.model.enums.PhotonBeamReaderOffsetType;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Extension of the consumer that allows for a multi-region implementation of photon.
 */
public interface PhotonMultiRegionConsumer extends PhotonConsumer {

    /**
     * Method for add a multi-region configuration to be scheduled.
     *
     * @param clientName - The name assigned to the reader by the end-user app.
     * @param beamName - The name of the beam to be read from.
     * @param photonMessageHandler - The implementation of the PhotonMessageHandler interface for callbacks upon receipt of a message.
     * @param offsetType - The type of offset to use for the reader.
     * @param multiRegionBackoff - The farthest back to look for replicated messages from other regions.
     * @param multiRegionBackoffUnit - The unit of the farthest back to look for replicated messages from other regions.
     */
    void putBeamForProcessing(String clientName, String beamName, PhotonMessageHandler photonMessageHandler,
                              PhotonBeamReaderOffsetType offsetType, long multiRegionBackoff, ChronoUnit multiRegionBackoffUnit);

    /**
     *
     * @param clientName - The name assigned to the reader by the end-user app.
     * @param beamName - The name of the beam to be read from.
     * @param photonMessageHandler - The implementation of the PhotonMessageHandler interface for callbacks upon receipt of a message.
     * @param offsetType - The type of offset to use for the reader.
     * @param offset - The Instant offset to begin reading from.
     * @param multiRegionBackoff - The farthest back to look for replicated messages from other regions.
     * @param multiRegionBackoffUnit - The unit of the farthest back to look for replicated messages from other regions.
     */
    void putBeamForProcessing(String clientName, String beamName, PhotonMessageHandler photonMessageHandler,
                              PhotonBeamReaderOffsetType offsetType, Instant offset, long multiRegionBackoff, ChronoUnit multiRegionBackoffUnit);

}
