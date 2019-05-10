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
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

@Slf4j
public class LockUpdateScheduler extends AbstractPhotonScheduler {

    private final ExecutorService executorService;
    private final BeamReaderCache beamReaderCache;
    private final BeamReaderLockDao beamReaderLockDao;
    private Duration lockThreshold;

    public LockUpdateScheduler(final ExecutorService executorService,
                               final BeamReaderCache beamReaderCache,
                               final BeamReaderLockDao beamReaderLockDao,
                               final ScheduledExecutorService scheduledExecutorService,
                               Duration lockThreshold) {
        super(scheduledExecutorService, lockThreshold.dividedBy(2));
        this.executorService = executorService;
        this.beamReaderCache = beamReaderCache;
        this.beamReaderLockDao = beamReaderLockDao;
        this.lockThreshold = lockThreshold;
    }

    @Override
    void executeTask() {
        beamReaderCache.getCacheAsMap()
                .values()
                .stream()
                .flatMap(v -> v.values().stream())
                .filter(br -> br.getPhotonBeamReader().getPhotonBeamReaderLock().isPresent())
                .forEach(br -> executorService.execute(() -> {
                    try {
                        if (br.getConsumerGroupLock().tryLock()) {
                            if (br.getPhotonBeamReader().getPhotonBeamReaderLock().isPresent()) {
                                if (br.getPhotonBeamReader().getPhotonBeamReaderLock().get().getLockTime()
                                        .isBefore(Instant.now().minusMillis(lockThreshold.toMillis()))) {
                                    br.getPhotonBeamReader().setPhotonBeamReaderLock(null);
                                } else {
                                    br.getPhotonBeamReader().getPhotonBeamReaderLock().get().setLockTime(Instant.now());
                                    beamReaderLockDao.updateLock(br.getPhotonBeamReader());
                                }
                            }
                        }
                    } catch (Exception e) {
                        log.error("Could not update beam reader lock.", e);
                    } finally {
                        if (br.getConsumerGroupLock().isHeldByCurrentThread()) {
                            br.getConsumerGroupLock().unlock();
                        }
                    }
                }));
    }
}
