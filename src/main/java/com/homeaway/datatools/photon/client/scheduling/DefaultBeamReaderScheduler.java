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
package com.homeaway.datatools.photon.client.scheduling;

import com.google.common.collect.Maps;
import com.homeaway.datatools.photon.api.beam.PhotonMessageHandler;
import com.homeaway.datatools.photon.api.model.PhotonBeamReaderLockWrapper;
import com.homeaway.datatools.photon.client.cache.BeamCache;
import com.homeaway.datatools.photon.client.cache.BeamReaderCache;
import com.homeaway.datatools.photon.client.consumer.partition.consumer.BeamConsumer;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import com.homeaway.datatools.photon.utils.client.consumer.BeamReaderConfigManager;
import com.homeaway.datatools.photon.utils.client.functions.BeamReaderExecutionFunction;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class DefaultBeamReaderScheduler extends AbstractPhotonScheduler {

    private static final Duration DEFAULT_POLLING_INTERVAL = Duration.ofMillis(200L);
    private final BeamReaderConfigManager beamReaderConfigManager;
    private final BeamCache beamCache;
    private final BeamReaderCache beamReaderCache;
    private final BeamReaderExecutionFunction execution;
    private final BeamConsumer beamConsumer;
    private final ConcurrentMap<String, Instant> beamReaderLastScheduled;


    public DefaultBeamReaderScheduler(final BeamReaderConfigManager beamReaderConfigManager,
                                      final BeamCache beamCache,
                                      final BeamReaderCache beamReaderCache,
                                      final BeamConsumer beamConsumer,
                                      final BeamReaderExecutionFunction execution) {
        this(beamReaderConfigManager, beamCache, beamReaderCache, beamConsumer, execution, Maps.newConcurrentMap());
    }

    public DefaultBeamReaderScheduler(final BeamReaderConfigManager beamReaderConfigManager,
                                      final BeamCache beamCache,
                                      final BeamReaderCache beamReaderCache,
                                      final BeamConsumer beamConsumer,
                                      final BeamReaderExecutionFunction execution,
                                      final ConcurrentMap<String, Instant> beamReaderLastScheduled) {
        super(DEFAULT_POLLING_INTERVAL);
        this.beamReaderConfigManager = beamReaderConfigManager;
        this.beamCache = beamCache;
        this.beamReaderCache = beamReaderCache;
        this.beamConsumer = beamConsumer;
        this.execution = execution;
        this.beamReaderLastScheduled = beamReaderLastScheduled;
    }

    @Override
    void executeTask() {
        beamReaderConfigManager.iterateConfigs(photonBeamReaderConfig -> {
            PhotonMessageHandler photonMessageHandler = photonBeamReaderConfig.getPhotonMessageHandler();
            Optional<PhotonBeam> beam = Optional.ofNullable(beamCache.getBeamByName(photonBeamReaderConfig.getBeamName()).peek());
            beamReaderLastScheduled.put(photonBeamReaderConfig.getClientName(), Instant.now());
            if (beam.isPresent()) {
                Optional<PhotonBeamReaderLockWrapper> wrapper = beamReaderCache.getPhotonBeamReader(photonBeamReaderConfig.getClientName(), beam.get());
                if (wrapper.isPresent()) {
                    if (wrapper.get().getPhotonBeamReader().getPhotonBeamReaderLock().isPresent()) {
                        if (!wrapper.get().getLock().isLocked()) {
                            getExecutorService(100).execute(() ->
                                    execution.execute(wrapper.get(), photonMessageHandler, beamCache, beamReaderConfigManager, beamConsumer, photonBeamReaderConfig.getIsAsync()));
                        }
                    }
                } else {
                    log.info("Creating new beam reader for client {} and beam {}", photonBeamReaderConfig.getClientName(),
                            photonBeamReaderConfig.getBeamName());
                    beamReaderConfigManager.createNewReader(photonBeamReaderConfig);
                }
            }
        });
    }
}
