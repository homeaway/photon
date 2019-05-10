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
package com.homeaway.datatools.photon.utils.client;

import com.homeaway.datatools.photon.client.consumer.PhotonBeamReaderConfig;
import com.homeaway.datatools.photon.dao.rows.PhotonRowSet;
import com.homeaway.datatools.photon.dao.rows.PhotonRowSetFuture;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import com.homeaway.datatools.photon.utils.client.functions.BeamReaderExecutionFunction;
import com.homeaway.datatools.photon.utils.client.functions.WaterMarkCheckFunction;
import static java.util.concurrent.TimeUnit.SECONDS;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

@Slf4j
public final class ConsumerUtils {

    private static final Integer RESULTSET_FUTURE_TIMEOUT = 5;

    public static final Function<PhotonRowSetFuture, PhotonRowSet> GET_PHOTON_ROWSET_FROM_FUTURE = (future) -> {
        try {
            return future.getUninterruptibly(RESULTSET_FUTURE_TIMEOUT, SECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            log.error("Timed out waiting for ResultSetFuture.", e);
            throw new RuntimeException(e);
        }
    };

    public static final Function<Queue<PhotonBeam>, PhotonBeam> GET_BEAM_WITH_EARLIEST_START = (beams) -> {
        if (!beams.isEmpty()) {
            PhotonBeam currentBeam = null;
            for(PhotonBeam beam : beams) {
                if (currentBeam == null
                        || beam.getStartDate().isBefore(currentBeam.getStartDate())) {
                    currentBeam = beam;
                }
            }
            return currentBeam;
        }
        return null;
    };


    public static final WaterMarkCheckFunction GET_WATERMARK = ((photonBeamReaderLockWrapper, beamCache, beamReaderConfigManager) -> {
        Optional<PhotonBeam> beam = beamCache.getBeamByUuid(photonBeamReaderLockWrapper.getPhotonBeamReader().getBeamUuid());
        if (beam.isPresent()) {
            Optional<PhotonBeamReaderConfig> config = beamReaderConfigManager
                    .getBeamReaderConfig(photonBeamReaderLockWrapper.getPhotonBeamReader().getClientName(), beam.get().getBeamName());
            if (config.isPresent()) {
                return Optional.of(config.get().getWaterMarkGetter().apply(photonBeamReaderLockWrapper.getPhotonBeamReader().getClientName(), beam.get().getBeamName()));
            }
        }
        return Optional.empty();
    });

    public static final BeamReaderExecutionFunction CONSUMER_EXECUTION_FUNCTION =
            (photonBeamReaderLockWrapper, photonMessageHandler, beamCache, configManager, beamConsumer, isAsync) -> {
                try {
                    if (photonBeamReaderLockWrapper.getLock().tryLock()) {
                        if (photonBeamReaderLockWrapper.getPhotonBeamReader().getWaterMark() == null) {
                            Optional<Instant> waterMarkOptional = GET_WATERMARK.get(photonBeamReaderLockWrapper, beamCache, configManager);
                            waterMarkOptional.ifPresent(w -> {
                                photonBeamReaderLockWrapper.getPhotonBeamReader().setCurrentWaterMark(w);
                                photonBeamReaderLockWrapper.getPhotonBeamReader().setWaterMark(w);
                            });
                        }
                        beamConsumer.consume(photonBeamReaderLockWrapper.getPhotonBeamReader(), photonMessageHandler, isAsync);

                    }
                } catch (Exception e) {
                    log.error("Could not poll beam {}", photonBeamReaderLockWrapper.getPhotonBeamReader().getBeamUuid(), e);
                } finally {
                    if (photonBeamReaderLockWrapper.getLock().isHeldByCurrentThread()) {
                        photonBeamReaderLockWrapper.getLock().unlock();
                    }
                }
            };


}


