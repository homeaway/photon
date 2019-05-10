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

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class AbstractPhotonScheduler implements PhotonScheduler {

    private final ScheduledExecutorService scheduledExecutorService;
    private volatile Duration pollingInterval;
    private ScheduledFuture<?> scheduledFuture;

    public AbstractPhotonScheduler(final ScheduledExecutorService scheduledExecutorService,
                                   final Duration pollingInterval) {
        this.scheduledExecutorService = scheduledExecutorService;
        this.pollingInterval = pollingInterval;
    }

    @Override
    public Boolean isActive() {
        return Optional.ofNullable(scheduledFuture).map(this::isActive).orElse(false);
    }

    @Override
    public Duration getPollingInterval() {
        return pollingInterval;
    }

    @Override
    public void setPollingInterval(Duration pollingInterval) {
        this.pollingInterval = pollingInterval;
        try {
            stop();
            start();
        } catch (Exception e) {
            log.error("Could not restart beam reader scheduler");
            throw new RuntimeException(e);
        }
    }

    @Override
    public void start() throws Exception {
        if (!isActive()) {
            scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(() ->  {
                try {
                    executeTask();
                } catch (Exception e) {
                    log.error("Could not schedule task {}", e);
                }
            }, 0L, pollingInterval.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void stop() throws Exception {
        if (isActive()) {
            scheduledFuture.cancel(true);
        }
    }

    abstract void executeTask();

    private boolean isActive(ScheduledFuture<?> scheduledFuture) {
        return !(scheduledFuture.isCancelled() || scheduledFuture.isDone());
    }
}
