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
package com.homeaway.datatools.photon.client.consumer;

import com.homeaway.datatools.photon.api.beam.PhotonMessageHandler;
import com.homeaway.datatools.photon.api.model.enums.PhotonBeamReaderOffsetType;
import lombok.Getter;

import java.time.Instant;
import java.util.function.BiFunction;

public class PhotonBeamReaderConfig {

    @Getter
    private final String clientName;

    @Getter
    private final String beamName;

    @Getter
    private final PhotonMessageHandler photonMessageHandler;

    @Getter
    private final PhotonBeamReaderOffsetType offsetType;

    @Getter
    private final BiFunction<String, String, Instant> waterMarkGetter;

    @Getter
    private final Boolean isAsync;

    public PhotonBeamReaderConfig(final String clientName,
                                  final String beamName,
                                  final PhotonMessageHandler photonMessageHandler,
                                  final PhotonBeamReaderOffsetType offsetType,
                                  final BiFunction<String, String, Instant> waterMarkGetter) {
        this(clientName, beamName, photonMessageHandler, offsetType, waterMarkGetter, Boolean.FALSE);
    }

    public PhotonBeamReaderConfig(final String clientName,
                                  final String beamName,
                                  final PhotonMessageHandler photonMessageHandler,
                                  final PhotonBeamReaderOffsetType offsetType,
                                  final BiFunction<String, String, Instant> waterMarkGetter,
                                  final Boolean isAsync) {
        this.clientName = clientName;
        this.beamName = beamName;
        this.photonMessageHandler = photonMessageHandler;
        this.offsetType = offsetType;
        this.waterMarkGetter = waterMarkGetter;
        this.isAsync = isAsync;
    }
}
