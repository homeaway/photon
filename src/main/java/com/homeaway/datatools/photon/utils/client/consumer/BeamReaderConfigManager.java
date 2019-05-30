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
package com.homeaway.datatools.photon.utils.client.consumer;

import com.homeaway.datatools.photon.client.consumer.PhotonBeamReaderConfig;

import java.util.Optional;
import java.util.function.Consumer;

public interface BeamReaderConfigManager {

    void putBeamReaderConfig(PhotonBeamReaderConfig photonBeamReaderConfig);

    PhotonBeamReaderConfig removeBeamReaderConfig(String clientName, String beamName);

    Optional<PhotonBeamReaderConfig> getBeamReaderConfig(String clientName, String beamName);

    void createNewReader(PhotonBeamReaderConfig photonBeamReaderConfig);

    void iterateConfigs(Consumer<PhotonBeamReaderConfig> callback);
}
