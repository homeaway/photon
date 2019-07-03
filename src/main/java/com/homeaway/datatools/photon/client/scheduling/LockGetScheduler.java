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

import com.homeaway.datatools.photon.client.cache.BeamReaderCache;
import com.homeaway.datatools.photon.dao.beam.BeamReaderLockDao;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReaderLock;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

@Slf4j
public class LockGetScheduler extends AbstractPhotonScheduler {

    private final ExecutorService executorService;
    private final BeamReaderCache beamReaderCache;
    private final BeamReaderLockDao beamReaderLockDao;

    public LockGetScheduler(final ExecutorService executorService,
                            final BeamReaderCache beamReaderCache,
                            final BeamReaderLockDao beamReaderLockDao,
                            final ScheduledExecutorService scheduledExecutorService,
                            Duration lockThreshold) {
        super(scheduledExecutorService, lockThreshold);
        this.executorService = executorService;
        this.beamReaderCache = beamReaderCache;
        this.beamReaderLockDao = beamReaderLockDao;
    }

    @Override
    void executeTask() {
        beamReaderCache.getCacheAsMap()
                .values()
                .stream()
                .flatMap(v -> v.values().stream())
                .filter(br -> !br.getPhotonBeamReader().getPhotonBeamReaderLock().isPresent())
                .forEach(br -> executorService.execute(() -> {
                    try {
                        Optional<PhotonBeamReaderLock> lock = beamReaderLockDao.getAvailablePhotonBeamLock(br.getPhotonBeamReader());
                        if (lock.isPresent()) {
                            lock.get().setLockTime(Instant.ofEpochMilli(Instant.now().toEpochMilli()));
                            lock.get().setLockUuid(UUID.randomUUID());

                            PhotonBeamReaderLock newLock = beamReaderLockDao.putBeamReaderLock(br.getPhotonBeamReader().getClientName(),
                                    br.getPhotonBeamReader().getBeamUuid(),
                                    lock.get());
                            if (lock.get().equals(newLock)) {
                                br.getPhotonBeamReader().setPhotonBeamReaderLock(newLock);
                            }
                        }
                    } catch (Exception e) {
                        log.error("Could not get beam reader lock.", e);
                    }
                }));
    }

    @Override
    void doShutDown() throws Exception {
        executorService.shutdown();
    }
}
