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

import com.homeaway.datatools.photon.api.model.PhotonBeamReaderLockWrapper;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

public interface BeamReaderCache {

    Optional<PhotonBeamReaderLockWrapper> getPhotonBeamReader(String clientName, PhotonBeam beam);

    void ejectPhotonBeamReader(String clientName, PhotonBeam beam);

    void putPhotonBeamReader(PhotonBeam photonBeam, PhotonBeamReader photonBeamReader);

    ConcurrentMap<String, ConcurrentMap<UUID, PhotonBeamReaderLockWrapper>> getCacheAsMap();
}
