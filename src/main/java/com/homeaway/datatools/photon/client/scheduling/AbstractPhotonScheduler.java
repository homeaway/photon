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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class AbstractPhotonScheduler implements PhotonScheduler {

    private static ExecutorService executorService;
    private static ScheduledExecutorService scheduledExecutorService;
    private volatile Duration pollingInterval;
    private ScheduledFuture<?> scheduledFuture;

    public AbstractPhotonScheduler(final Duration pollingInterval) {
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
            scheduledFuture = getScheduledExecutorService().scheduleAtFixedRate(() ->  {
                try {
                    executeTask();
                } catch (Exception e) {
                    log.error("Could not schedule task", e);
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

    private boolean isActive(ScheduledFuture<?> scheduledFuture) {
        return !(scheduledFuture.isCancelled() || scheduledFuture.isDone());
    }

    @Override
    public void shutdown() throws Exception {
        Optional.ofNullable(scheduledExecutorService)
                .ifPresent(ScheduledExecutorService::shutdown);
        Optional.ofNullable(executorService)
                .ifPresent(ExecutorService::shutdown);
    }

    private ScheduledExecutorService getScheduledExecutorService() {
        return Optional.ofNullable(scheduledExecutorService)
                .filter(s -> !s.isShutdown())
                .orElseGet(() -> {
                    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
                    return scheduledExecutorService;
                });
    }

    protected ExecutorService getExecutorService(int poolSize) {
        return Optional.ofNullable(executorService)
                .filter(e -> !e.isShutdown())
                .orElseGet(() -> {
                    executorService = Executors.newFixedThreadPool(poolSize);
                    return executorService;
                });
    }


    abstract void executeTask();

}
