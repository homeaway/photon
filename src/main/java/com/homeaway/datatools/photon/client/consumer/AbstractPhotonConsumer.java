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

import com.google.common.collect.Iterables;
import com.homeaway.datatools.photon.api.beam.PhotonConsumer;
import com.homeaway.datatools.photon.api.beam.PhotonMessageHandler;
import com.homeaway.datatools.photon.client.cache.BeamCache;
import com.homeaway.datatools.photon.client.cache.BeamReaderCache;
import com.homeaway.datatools.photon.client.consumer.partition.consumer.BeamConsumer;
import com.homeaway.datatools.photon.client.consumer.partition.consumer.WalkBackBeamConsumer;
import com.homeaway.datatools.photon.client.scheduling.PhotonScheduler;
import com.homeaway.datatools.photon.dao.beam.BeamReaderDao;
import com.homeaway.datatools.photon.api.model.enums.PhotonBeamReaderOffsetType;
import static com.homeaway.datatools.photon.utils.client.ConsumerUtils.GET_BEAM_WITH_EARLIEST_START;
import com.homeaway.datatools.photon.utils.client.consumer.BeamReaderConfigManager;
import com.homeaway.datatools.photon.utils.client.consumer.BeamReaderLockManager;
import static java.lang.Boolean.TRUE;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

@Slf4j
abstract class AbstractPhotonConsumer implements PhotonConsumer {

    protected final BeamReaderConfigManager beamReaderConfigManager;
    protected final BeamCache beamCache;
    private final BeamConsumer beamConsumer;
    private final BeamReaderDao beamReaderDao;
    private final BeamReaderCache beamReaderCache;
    private final PhotonScheduler beamReaderScheduler;
    private final BeamReaderLockManager beamReaderLockManager;
    private volatile boolean active;
    final WalkBackBeamConsumer walkBackBeamConsumer;

    public AbstractPhotonConsumer(final BeamReaderConfigManager beamReaderConfigManager,
                                  final PhotonScheduler beamReaderScheduler,
                                  final BeamReaderLockManager beamReaderLockManager,
                                  final BeamCache beamCache,
                                  final BeamReaderCache beamReaderCache,
                                  final BeamReaderDao beamReaderDao,
                                  final BeamConsumer beamConsumer) {
        this(beamReaderConfigManager, beamReaderScheduler, beamReaderLockManager, beamCache, beamReaderCache, beamReaderDao, beamConsumer, null);
    }

    public AbstractPhotonConsumer(final BeamReaderConfigManager beamReaderConfigManager,
                                  final PhotonScheduler beamReaderScheduler,
                                  final BeamReaderLockManager beamReaderLockManager,
                                  final BeamCache beamCache,
                                  final BeamReaderCache beamReaderCache,
                                  final BeamReaderDao beamReaderDao,
                                  final BeamConsumer beamConsumer,
                                  final WalkBackBeamConsumer walkBackBeamConsumer) {
        this.beamReaderConfigManager = beamReaderConfigManager;
        this.beamReaderScheduler = beamReaderScheduler;
        this.beamReaderLockManager = beamReaderLockManager;
        this.beamCache = beamCache;
        this.beamReaderCache = beamReaderCache;
        this.beamReaderDao = beamReaderDao;
        this.beamConsumer = beamConsumer;
        this.walkBackBeamConsumer = walkBackBeamConsumer;
        this.active = false;
    }

    @Override
    public void setPollingInterval(Long pollingInterval) {
        if (pollingInterval >= 0) {
            this.beamReaderScheduler.setPollingInterval(Duration.ofMillis(pollingInterval));
        } else {
            throw new RuntimeException(new Exception("Cannot set polling interval below 0"));
        }
    }

    @Override
    public Long getPollingInterval() {
        return beamReaderScheduler.getPollingInterval().toMillis();
    }

    @Override
    public void start() throws Exception {
        if (!active) {
            beamReaderScheduler.start();
            beamReaderLockManager.start();
            Optional.ofNullable(walkBackBeamConsumer).ifPresent(wc -> {
                    try {
                        wc.start();
                    } catch (Exception e) {
                        log.error("Could not start walkback consumer", e);
                        throw new RuntimeException(e);
                    }
            });
            active = true;
            log.info("Beam consumer started.");
        }
    }

    @Override
    public void stop() throws Exception {
        if (active) {
            beamReaderScheduler.stop();
            Optional.ofNullable(walkBackBeamConsumer).ifPresent(wc -> {
                try {
                    wc.stop();
                } catch (Exception e) {
                    log.error("Could not stop walkback consumer", e);
                    throw new RuntimeException(e);
                }
            });
            beamReaderCache.getCacheAsMap()
                    .values()
                    .stream()
                    .flatMap(v -> v.values().stream())
                    .filter(br -> br.getLock().isLocked())
                    .forEach(br -> beamConsumer.stopConsumers(br.getPhotonBeamReader().getBeamReaderUuid()));
            beamReaderLockManager.stop();
            this.active = false;
        }
    }

    @Override
    public void putBeamForProcessing(String clientName, String beamName, PhotonMessageHandler photonMessageHandler,
                                     PhotonBeamReaderOffsetType offsetType) {
        putBeamForProcessing(clientName, beamName, photonMessageHandler, offsetType, getDefaultWatermarkFunction(offsetType));
    }

    @Override
    public void putBeamForProcessing(String clientName, String beamName, PhotonMessageHandler photonMessageHandler,
                                     PhotonBeamReaderOffsetType offsetType, Instant offset) {
        putBeamForProcessing(clientName, beamName, photonMessageHandler, offsetType, (client, beam) -> {
                    Instant start = GET_BEAM_WITH_EARLIEST_START.apply(beamCache.getBeamByName(beamName))
                            .getStartDate();
                    return offset.isBefore(start) ? start : offset;
                });
    }

    @Override
    public void putBeamForProcessing(String clientName,
                                     String beamName,
                                     PhotonMessageHandler photonMessageHandler,
                                     PhotonBeamReaderOffsetType offsetType,
                                     BiFunction<String, String, Instant> waterMarkGetter) {
        PhotonBeamReaderConfig config = new PhotonBeamReaderConfig(clientName, beamName, photonMessageHandler,
                offsetType, waterMarkGetter);
        beamReaderConfigManager.putBeamReaderConfig(config);
    }

    @Override
    public void putBeamForAsyncProcessing(String clientName,
                                          String beamName,
                                          PhotonMessageHandler photonMessageHandler,
                                          PhotonBeamReaderOffsetType offsetType){
        PhotonBeamReaderConfig config = new PhotonBeamReaderConfig(clientName, beamName, photonMessageHandler,
                offsetType, getDefaultWatermarkFunction(offsetType), TRUE);
        beamReaderConfigManager.putBeamReaderConfig(config);
    }

    @Override
    public void putBeamForAsyncProcessing(String clientName, String beamName, PhotonMessageHandler photonMessageHandler,
                                          PhotonBeamReaderOffsetType offsetType, BiFunction<String, String, Instant> waterMarkGetter){
        PhotonBeamReaderConfig config = new PhotonBeamReaderConfig(clientName, beamName, photonMessageHandler,
                offsetType, waterMarkGetter, TRUE);
        beamReaderConfigManager.putBeamReaderConfig(config);
    }

    @Override
    public void removeBeamFromProcessing(String clientName, String beamName) {
        beamReaderConfigManager.removeBeamReaderConfig(clientName, beamName);
    }


    BiFunction<String, String, Instant> getDefaultWatermarkFunction(PhotonBeamReaderOffsetType offsetType) {
        if (offsetType.equals(PhotonBeamReaderOffsetType.FROM_BEGINNING)) {
            return (clientName, beamName) -> GET_BEAM_WITH_EARLIEST_START.apply(beamCache.getBeamByName(beamName)).getStartDate();
        } else if (offsetType.equals(PhotonBeamReaderOffsetType.FROM_CURRENT)) {
            return (clientName, beamName) -> beamReaderDao.getWatermark(Iterables.getOnlyElement(beamCache.getBeamByName(beamName)
                        .stream()
                        .map(b -> beamReaderCache.getPhotonBeamReader(clientName, b))
                        .filter(Optional::isPresent)
                        .collect(Collectors.toList())
                        .stream()
                        .map(br -> br.get().getPhotonBeamReader())
                        .collect(Collectors.toList()))).orElse(GET_BEAM_WITH_EARLIEST_START.apply(beamCache.getBeamByName(beamName)).getStartDate());
        } else if (offsetType.equals(PhotonBeamReaderOffsetType.FROM_OFFSET)) {
            return (clientName, beamName) -> Instant.now();
        }
        return null;
    }

}
