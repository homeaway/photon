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

import com.homeaway.datatools.photon.client.cache.BeamCache;
import com.homeaway.datatools.photon.client.cache.BeamReaderCache;
import com.homeaway.datatools.photon.client.consumer.partition.consumer.BeamConsumer;
import com.homeaway.datatools.photon.client.scheduling.DefaultBeamReaderScheduler;
import com.homeaway.datatools.photon.client.scheduling.PhotonScheduler;
import com.homeaway.datatools.photon.dao.beam.BeamReaderDao;
import com.homeaway.datatools.photon.utils.client.ConsumerUtils;
import com.homeaway.datatools.photon.utils.client.consumer.BeamReaderConfigManager;
import com.homeaway.datatools.photon.utils.client.consumer.BeamReaderLockManager;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultPhotonConsumer extends AbstractPhotonConsumer {

    public DefaultPhotonConsumer(final BeamReaderConfigManager beamReaderConfigManager,
                                 final BeamReaderLockManager beamReaderLockManager,
                                 final BeamCache beamCache,
                                 final BeamReaderCache beamReaderCache,
                                 final BeamReaderDao beamReaderDao,
                                 final BeamConsumer beamConsumer) {
        this(beamReaderConfigManager,
                new DefaultBeamReaderScheduler(beamReaderConfigManager, beamCache, beamReaderCache, beamConsumer, ConsumerUtils.CONSUMER_EXECUTION_FUNCTION),
                beamReaderLockManager, beamCache, beamReaderCache, beamReaderDao, beamConsumer);
    }

    public DefaultPhotonConsumer(final BeamReaderConfigManager beamReaderConfigManager,
                                 final PhotonScheduler beamReaderScheduler,
                                 final BeamReaderLockManager beamReaderLockManager,
                                 final BeamCache beamCache,
                                 final BeamReaderCache beamReaderCache,
                                 final BeamReaderDao beamReaderDao,
                                 final BeamConsumer beamConsumer) {
        super(beamReaderConfigManager, beamReaderScheduler, beamReaderLockManager, beamCache, beamReaderCache, beamReaderDao,
                beamConsumer);
    }
}
