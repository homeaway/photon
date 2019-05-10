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

import com.google.common.collect.Maps;
import com.homeaway.datatools.photon.client.cache.BeamCache;
import com.homeaway.datatools.photon.client.cache.BeamReaderCache;
import com.homeaway.datatools.photon.client.consumer.PhotonBeamReaderConfig;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;

import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

public class DefaultBeamReaderConfigManager implements BeamReaderConfigManager {

    private final BeamCache beamCache;
    private final BeamReaderCache beamReaderCache;
    private final ConcurrentMap<String, ConcurrentMap<String, PhotonBeamReaderConfig>> beamConfigs;

    public DefaultBeamReaderConfigManager(final BeamCache beamCache,
                                          final BeamReaderCache beamReaderCache) {
        this(beamCache, beamReaderCache, Maps.newConcurrentMap());
    }

    public DefaultBeamReaderConfigManager(final BeamCache beamCache,
                                          final BeamReaderCache beamReaderCache,
                                          final ConcurrentMap<String, ConcurrentMap<String, PhotonBeamReaderConfig>> beamConfigs) {
        this.beamCache = beamCache;
        this.beamReaderCache = beamReaderCache;
        this.beamConfigs = beamConfigs;
    }

    @Override
    public void putBeamReaderConfig(PhotonBeamReaderConfig photonBeamReaderConfig) {
        if(photonBeamReaderConfig.getOffsetType().getNewReader()) {
            createNewReader(photonBeamReaderConfig);
        }
        beamConfigs.computeIfAbsent(photonBeamReaderConfig.getClientName(), k -> Maps.newConcurrentMap())
                .put(photonBeamReaderConfig.getBeamName(), photonBeamReaderConfig);

    }

    @Override
    public ConcurrentMap<String, PhotonBeamReaderConfig> removeBeamReaderConfig(String clientName) {
        return beamConfigs.remove(clientName);
    }

    @Override
    public Optional<PhotonBeamReaderConfig> getBeamReaderConfig(String clientName, String beamName) {
        return Optional.ofNullable(beamConfigs.getOrDefault(clientName, Maps.newConcurrentMap()).get(beamName));
    }

    @Override
    public void createNewReader(PhotonBeamReaderConfig photonBeamReaderConfig) {
        Queue<PhotonBeam> beams = beamCache.getBeamByName(photonBeamReaderConfig.getBeamName());
        if (!beams.isEmpty()) {
            PhotonBeamReader photonBeamReader = new PhotonBeamReader();
            photonBeamReader.setClientName(photonBeamReaderConfig.getClientName());
            photonBeamReader.setBeamReaderUuid(UUID.randomUUID());
            photonBeamReader.setBeamUuid(beams.peek().getBeamUuid());
            beamReaderCache.putPhotonBeamReader(beams.peek(), photonBeamReader);
        }
    }

    @Override
    public void iterateConfigs(Consumer<PhotonBeamReaderConfig> callback) {
        beamConfigs.entrySet()
                .forEach(e -> e.getValue()
                        .entrySet()
                        .forEach(es -> callback.accept(es.getValue())));
    }
}
