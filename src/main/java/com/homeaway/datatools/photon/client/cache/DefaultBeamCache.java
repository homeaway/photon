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
import com.google.common.collect.Lists;
import com.homeaway.datatools.photon.dao.beam.BeamDao;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DefaultBeamCache implements BeamCache {

    private static final long MAX_CACHE_SIZE = 500;
    private final LoadingCache<String, Queue<PhotonBeam>> cacheByName;
    private final LoadingCache<UUID, Optional<PhotonBeam>> cacheByUuid;
    private final BeamDao beamDao;

    public DefaultBeamCache(final BeamDao beamDao) {
        this.beamDao = beamDao;
        cacheByName = CacheBuilder.newBuilder()
                .maximumSize(MAX_CACHE_SIZE)
                .expireAfterWrite(5L, TimeUnit.SECONDS)
                .build(new CacheLoader<String, Queue<PhotonBeam>>() {
                    @Override
                    public Queue<PhotonBeam> load(String key) throws Exception {
                        Queue<PhotonBeam> beams = beamDao.getBeamByName(key);
                        if (!beams.isEmpty()) {
                            return beams;
                        } else {
                            throw new BeamNotFoundException(String.format("Could not find beam [%s]", key));
                        }
                    }
                });

        cacheByUuid = CacheBuilder.newBuilder()
                .maximumSize(MAX_CACHE_SIZE)
                .build(new CacheLoader<UUID, Optional<PhotonBeam>>() {
                    @Override
                    public Optional<PhotonBeam> load(UUID key) throws Exception {
                        return beamDao.getBeamByUuid(key);
                        }
                    });
    }

    @Override
    public Queue<PhotonBeam> getBeamByName(String beamName) {
        try {
            return cacheByName.get(beamName);
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof BeamNotFoundException)) {
                log.error("Could not fetch beam from cache {}", beamName, e);
            }
        }
        return Lists.newLinkedList();
    }

    @Override
    public Optional<PhotonBeam> getBeamByUuid(UUID beamUuid) {
        try {
            return cacheByUuid.get(beamUuid);
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof BeamNotFoundException)) {
                log.error("Could not fetch beam from cache {}", beamUuid, e);
            }
        }
        return Optional.empty();
    }

    @Override
    public void putBeam(PhotonBeam photonBeam) {
        beamDao.putBeam(photonBeam);
        cacheByUuid.put(photonBeam.getBeamUuid(), Optional.of(photonBeam));
    }

    @Override
    public Queue<PhotonBeam> getBeamByNameIfPresent(String beamName) {
        return Optional.ofNullable(cacheByName.getIfPresent(beamName)).orElse(Lists.newLinkedList());
    }
}
