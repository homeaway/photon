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
package com.homeaway.datatools.photon.client.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.homeaway.datatools.photon.api.model.PhotonBeamReaderLockWrapper;
import com.homeaway.datatools.photon.dao.beam.BeamReaderDao;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

@Slf4j
public class DefaultBeamReaderCache implements BeamReaderCache {

    private final ConcurrentMap<String, LoadingCache<UUID, PhotonBeamReaderLockWrapper>> readerCaches;
    private final BeamReaderDao beamReaderDao;

    public DefaultBeamReaderCache(final BeamReaderDao beamReaderDao) {
        this(beamReaderDao, Maps.newConcurrentMap());
    }

    public DefaultBeamReaderCache(final BeamReaderDao beamReaderDao,
                                  final ConcurrentMap<String, LoadingCache<UUID, PhotonBeamReaderLockWrapper>> readerCaches) {
        this.beamReaderDao = beamReaderDao;
        this.readerCaches = readerCaches;
    }

    @Override
    public Optional<PhotonBeamReaderLockWrapper> getPhotonBeamReader(String clientName, PhotonBeam beam) {
        try {
            return Optional.of(readerCaches.computeIfAbsent(clientName, this::buildCache).get(beam.getBeamUuid()));
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof BeamReaderNotFoundException)) {
                log.error("Could not get Beam Reader for client name = {} and beam = {}", clientName, beam, e);
            }
        }
        return Optional.empty();
    }

    @Override
    public void ejectPhotonBeamReader(String clientName, PhotonBeam beam) {
        readerCaches.getOrDefault(clientName, buildCache(clientName))
                .invalidate(beam.getBeamUuid());
    }

    @Override
    public void putPhotonBeamReader(PhotonBeam photonBeam, PhotonBeamReader photonBeamReader) {
        beamReaderDao.putBeamReader(photonBeamReader);
        readerCaches.computeIfAbsent(photonBeamReader.getClientName(), this::buildCache)
                .put(photonBeam.getBeamUuid(), PhotonBeamReaderLockWrapper.of(photonBeamReader));
    }

    @Override
    public ConcurrentMap<String, ConcurrentMap<UUID, PhotonBeamReaderLockWrapper>> getCacheAsMap() {
        ConcurrentMap<String, ConcurrentMap<UUID, PhotonBeamReaderLockWrapper>> cacheMap = Maps.newConcurrentMap();
        for(Map.Entry<String, LoadingCache<UUID, PhotonBeamReaderLockWrapper>> e : readerCaches.entrySet()) {
            cacheMap.put(e.getKey(), e.getValue().asMap());
        }
        return cacheMap;

    }

    private LoadingCache<UUID, PhotonBeamReaderLockWrapper> buildCache(String clientName) {
        return CacheBuilder.newBuilder()
                .build(new CacheLoader<UUID, PhotonBeamReaderLockWrapper>() {
                    @Override
                    public PhotonBeamReaderLockWrapper load(UUID beamUuid) throws Exception {
                        Optional<PhotonBeamReader> readerOptional = beamReaderDao.getBeamReaderByClientNameBeamUuid(clientName, beamUuid);
                        if (readerOptional.isPresent()) {
                            return PhotonBeamReaderLockWrapper.of(readerOptional.get());
                        } else {
                            throw new BeamReaderNotFoundException(String.format("Could not find beam reader for client = [%s], beam uuid = [%s]", clientName, beamUuid));
                        }
                    }
                });
    }
}
