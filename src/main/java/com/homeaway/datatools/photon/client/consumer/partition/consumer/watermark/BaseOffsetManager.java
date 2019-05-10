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
package com.homeaway.datatools.photon.client.consumer.partition.consumer.watermark;

import com.homeaway.datatools.photon.dao.beam.BeamReaderDao;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class BaseOffsetManager implements OffsetManager {

    private final BeamReaderDao beamReaderDao;
    private volatile Long offset;

    public BaseOffsetManager(final BeamReaderDao beamReaderDao) {
        this(beamReaderDao, Executors.newSingleThreadScheduledExecutor());
    }

    public BaseOffsetManager(final BeamReaderDao beamReaderDao,
                             final ScheduledExecutorService scheduledExecutorService) {
        this.beamReaderDao = beamReaderDao;
        this.offset = null;
        scheduledExecutorService.scheduleAtFixedRate(() -> offset = getCurrentOffset(), 0, 5, TimeUnit.MINUTES);
    }

    @Override
    public Instant getNowWithOffset() {
        return Instant.now().plusMillis(Optional.ofNullable(offset).orElseGet(this::getCurrentOffset));
    }

    private long getCurrentOffset() {
        Instant now = Instant.now();
        return now.until(beamReaderDao.getCurrentClusterTime(), ChronoUnit.MILLIS);
    }
}
