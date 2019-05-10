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

import com.google.common.collect.Maps;
import com.homeaway.datatools.photon.api.beam.BeamFuture;
import com.homeaway.datatools.photon.dao.beam.BeamReaderDao;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;
import com.homeaway.datatools.photon.utils.client.DefaultBeamFuture;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class DefaultWatermarkUpdater implements WatermarkUpdater {

    private final BeamReaderDao beamReaderDao;
    private final ConcurrentMap<UUID, BeamFuture> beamReaderUpdateFutures;

    public DefaultWatermarkUpdater(final BeamReaderDao beamReaderDao) {
        this(beamReaderDao, Maps.newConcurrentMap());
    }

    public DefaultWatermarkUpdater(final BeamReaderDao beamReaderDao,
                                   final ConcurrentMap<UUID, BeamFuture> beamReaderUpdateFutures) {
        this.beamReaderDao = beamReaderDao;
        this.beamReaderUpdateFutures = beamReaderUpdateFutures;
    }

    @Override
    public void updateWatermark(PhotonBeamReader photonBeamReader) {
        Optional<BeamFuture> future = Optional.ofNullable(beamReaderUpdateFutures.get(photonBeamReader.getBeamReaderUuid()));
        if (future.map(BeamFuture::isDone).orElse(Boolean.TRUE)) {
            beamReaderUpdateFutures.put(photonBeamReader.getBeamReaderUuid(), new DefaultBeamFuture(beamReaderDao.updateWatermark(photonBeamReader)));
        }
    }
}
