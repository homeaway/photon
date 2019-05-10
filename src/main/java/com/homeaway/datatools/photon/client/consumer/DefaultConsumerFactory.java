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

import com.homeaway.datatools.photon.api.beam.AsyncPhotonConsumer;
import com.homeaway.datatools.photon.api.beam.ConsumerFactory;
import com.homeaway.datatools.photon.api.beam.PhotonConsumer;
import com.homeaway.datatools.photon.client.cache.BeamCache;
import com.homeaway.datatools.photon.client.cache.BeamReaderCache;
import com.homeaway.datatools.photon.client.cache.ProcessedRecordCache;
import com.homeaway.datatools.photon.dao.beam.BeamReaderDao;
import com.homeaway.datatools.photon.utils.processing.DefaultProcessorManifest;
import com.homeaway.datatools.photon.utils.processing.ProcessorManifest;

import java.time.Instant;

public class DefaultConsumerFactory implements ConsumerFactory {

    private final BeamCache beamCache;
    private final BeamReaderCache beamReaderCache;
    private final BeamReaderDao beamReaderDao;
    private final PhotonConsumer photonConsumer;
    private final ProcessedRecordCache processedRecordCache;

    public DefaultConsumerFactory(final PhotonConsumer photonConsumer,
                                  final BeamCache beamCache,
                                  final BeamReaderCache beamReaderCache,
                                  final BeamReaderDao beamReaderDao,
                                  final ProcessedRecordCache processedRecordCache) {
        this.photonConsumer = photonConsumer;
        this.beamCache = beamCache;
        this.beamReaderCache = beamReaderCache;
        this.beamReaderDao = beamReaderDao;
        this.processedRecordCache = processedRecordCache;
    }

    @Override
    public PhotonConsumer getPhotonConsumer() {
        return photonConsumer;
    }

    @Override
    public <T> AsyncPhotonConsumer<T> getAsyncPhotonConsumer() {
        return getAsyncPhotonConsumer(0);
    }

    @Override
    public <T> AsyncPhotonConsumer<T> getAsyncPhotonConsumer(int maxConcurrentEvents) {
        ProcessorManifest<PhotonProcessorKey, PhotonProcessorEvent<T>, Instant> manifest =
                new DefaultProcessorManifest<>((key, writeTime) ->
                    beamReaderCache.getPhotonBeamReader(key.getClientName(), beamCache.getBeamByName(key.getBeamName()).peek())
                            .ifPresent(w -> {
                                if (writeTime.isAfter(w.getPhotonBeamReader().getWaterMark())) {
                                    w.getPhotonBeamReader().setWaterMark(writeTime);
                                    beamReaderDao.updateWatermark(w.getPhotonBeamReader());
                                }
                            }));

        return new DefaultAsyncPhotonConsumer<>(photonConsumer, manifest, processedRecordCache, maxConcurrentEvents);
    }

    @Override
    public void setPollingInterval(Long pollingInterval) {
        photonConsumer.setPollingInterval(pollingInterval);
    }

}
