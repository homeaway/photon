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
package com.homeaway.datatools.photon.client.cache;

import com.homeaway.datatools.photon.api.beam.PhotonMessage;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public interface ProcessedRecordCache {

    Optional<ProcessedRecordCacheEntry> getProcessedEntities(PhotonBeamReader photonBeamReader, Instant partitionTime);

    Optional<ProcessedRecordCacheEntry> getProcessedEntities(UUID beamReaderUuid, Instant partitionTime);

    void putEntry(UUID beamReaderUuid, Instant partitionTime, Instant writeTime, String messageKey, boolean persist);

    void putEntry(PhotonMessage photonMessage, Boolean persist);
}