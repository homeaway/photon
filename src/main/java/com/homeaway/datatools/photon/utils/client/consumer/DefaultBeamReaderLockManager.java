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

import com.homeaway.datatools.photon.client.cache.BeamReaderCache;
import com.homeaway.datatools.photon.client.scheduling.LockGetScheduler;
import com.homeaway.datatools.photon.client.scheduling.LockUpdateScheduler;
import com.homeaway.datatools.photon.client.scheduling.PhotonScheduler;
import com.homeaway.datatools.photon.dao.beam.BeamReaderLockDao;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Slf4j
public class DefaultBeamReaderLockManager implements BeamReaderLockManager {

    private final PhotonScheduler lockUpdateScheduler;
    private final PhotonScheduler lockGetScheduler;


    public DefaultBeamReaderLockManager(final BeamReaderCache beamReaderCache,
                                        final BeamReaderLockDao beamReaderLockDao,
                                        final Duration lockThreshold) {
        this(Executors.newScheduledThreadPool(5), Executors.newFixedThreadPool(50), beamReaderCache,
                beamReaderLockDao, lockThreshold);
    }

    public DefaultBeamReaderLockManager(final ScheduledExecutorService scheduledExecutorService,
                                        final ExecutorService executorService,
                                        final BeamReaderCache beamReaderCache,
                                        final BeamReaderLockDao beamReaderLockDao,
                                        final Duration lockThreshold) {
        this(new LockUpdateScheduler(executorService, beamReaderCache, beamReaderLockDao,
                        scheduledExecutorService, lockThreshold),
                new LockGetScheduler(executorService, beamReaderCache, beamReaderLockDao,
                        scheduledExecutorService, lockThreshold));
    }

    public DefaultBeamReaderLockManager(final PhotonScheduler lockUpdateScheduler,
                                        final PhotonScheduler lockGetScheduler) {
        this.lockUpdateScheduler = lockUpdateScheduler;
        this.lockGetScheduler = lockGetScheduler;
    }

    @Override
    public void start() throws Exception {
        lockUpdateScheduler.start();
        lockGetScheduler.start();
    }

    @Override
    public void stop() throws Exception {
        lockUpdateScheduler.stop();
        lockGetScheduler.stop();
    }

    @Override
    public void shutdown() throws Exception {
        lockUpdateScheduler.shutdown();
        lockGetScheduler.shutdown();
    }
}
