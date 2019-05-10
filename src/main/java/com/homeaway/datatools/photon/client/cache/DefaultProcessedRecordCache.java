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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;
import com.homeaway.datatools.photon.api.beam.PhotonMessage;
import com.homeaway.datatools.photon.dao.beam.BeamProcessedDao;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DefaultProcessedRecordCache implements ProcessedRecordCache {

    private static final long MAX_CACHE_SIZE = 100000L;
    private final ConcurrentMap<UUID, Cache<Instant, ProcessedRecordCacheEntry>> caches;
    private final BeamProcessedDao beamProcessedDao;

    public DefaultProcessedRecordCache(final BeamProcessedDao beamProcessedDao) {
        this(Maps.newConcurrentMap(), beamProcessedDao);
    }

    public DefaultProcessedRecordCache(final ConcurrentMap<UUID, Cache<Instant, ProcessedRecordCacheEntry>> caches,
                                       final BeamProcessedDao beamProcessedDao) {
        this.caches = caches;
        this.beamProcessedDao = beamProcessedDao;
    }

    @Override
    public Optional<ProcessedRecordCacheEntry> getProcessedEntities(PhotonBeamReader photonBeamReader, Instant partitionTime) {
        return getProcessedEntities(photonBeamReader.getBeamReaderUuid(), partitionTime);
    }

    @Override
    public Optional<ProcessedRecordCacheEntry> getProcessedEntities(UUID beamReaderUuid, Instant partitionTime) {
        return Optional.ofNullable(caches.computeIfAbsent(beamReaderUuid, k -> getCache())
                .getIfPresent(partitionTime));
    }

    @Override
    public void putEntry(UUID beamReaderUuid, Instant partitionTime, Instant writeTime, String messageKey, boolean persist) {
        try {
            caches.computeIfAbsent(beamReaderUuid, k -> getCache())
                    .get(partitionTime, DefaultProcessedRecordCacheEntry::new)
                    .putValue(writeTime, messageKey);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

        // Only persist the processed record if asked to do so, otherwise just keep in memory.
        if (persist) {
            beamProcessedDao.putProcessedMessage(beamReaderUuid, partitionTime, writeTime, messageKey);
        }
    }

    @Override
    public void putEntry(PhotonMessage photonMessage,
                         Boolean persist) {

        putEntry(photonMessage.getBeamReaderUuid(), photonMessage.getPartitionTime(), photonMessage.getWriteTime(), photonMessage.getMessageKey(),
                persist);
    }

    private Cache<Instant, ProcessedRecordCacheEntry> getCache() {
        return CacheBuilder
                .newBuilder()
                .expireAfterAccess(1L, TimeUnit.MINUTES)
                .maximumSize(MAX_CACHE_SIZE)
                .build();
    }
}
