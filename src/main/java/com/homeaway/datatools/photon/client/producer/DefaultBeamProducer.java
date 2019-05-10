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
package com.homeaway.datatools.photon.client.producer;

import com.google.common.collect.Lists;
import com.homeaway.datatools.photon.api.beam.BeamFuture;
import com.homeaway.datatools.photon.api.beam.BeamProducer;
import com.homeaway.datatools.photon.client.cache.BeamCache;
import com.homeaway.datatools.photon.dao.beam.WriteFuture;
import com.homeaway.datatools.photon.dao.beam.BeamDataDao;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import com.homeaway.datatools.photon.dao.model.message.PhotonProducerMessage;
import com.homeaway.datatools.photon.serialization.PhotonSerializer;
import com.homeaway.datatools.photon.utils.client.DefaultBeamFuture;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * The DefaultBeamProducer is an implementation of the BeamProducer that provides functionality to write messages to a
 * Cassandra backed beam.
 */
@Slf4j
public class DefaultBeamProducer implements BeamProducer {

    private final PhotonSerializer serializer;
    private final BeamDataDao beamDataDao;
    private final BeamCache beamCache;

    public DefaultBeamProducer(final BeamDataDao beamDataDao,
                               final BeamCache beamCache,
                               final PhotonSerializer serializer) {
        this.beamDataDao = beamDataDao;
        this.beamCache = beamCache;
        this.serializer = serializer;
    }

    @Override
    public void writeMessageToBeam(String beamName,
                                   String messageKey,
                                   Object payload) {
        writeMessageToBeam(beamName, messageKey, payload, Instant.now());
    }

    @Override
    public void writeMessageToBeam(String beamName,
                                   String messageKey,
                                   Object payload,
                                   Duration ttl) {
        writeMessageToBeam(beamName, messageKey, payload, Instant.now(), ttl);
    }

    @Override
    public void writeMessageToBeam(String beamName,
                                   String messageKey,
                                   Object payload,
                                   Instant writeTime) {
        try {
            writeMessageToBeamAsync(beamName, messageKey, payload, writeTime).get();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeMessageToBeam(String beamName,
                                   String messageKey,
                                   Object payload,
                                   Instant writeTime,
                                   Duration ttl) {
        try {
            writeMessageToBeamAsync(beamName, messageKey, payload, writeTime, ttl).get();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteMessageFromBeam(String beamName,
                                      String messageKey,
                                      Instant writeTime) {
        Queue<PhotonBeam> beams = beamCache.getBeamByName(beamName);
        if (beams.isEmpty()) {
            throw new BeamNotFoundException(String.format("Could not find beam {}", beamName));
        }
        List<WriteFuture> futures = Lists.newArrayList();
        beams.forEach(b -> futures.add(beamDataDao.deleteMessageAsync(b.getBeamUuid(), writeTime, messageKey)));
        for (WriteFuture f : futures) {
            if (!f.getUninterruptibly()) {
                throw new RuntimeException(String.format("Could not delete message key = %s and writetime = %s from beam = %s",
                        messageKey, writeTime, beamName));
            }
        }
    }

    @Override
    public BeamFuture writeMessageToBeamAsync(String beamName,
                                              String messageKey,
                                              Object payload) {
        PhotonBeam beam = getPhotonBeam(beamName, null);
        return writeMessageToBeamAsync(beam, messageKey, payload, Instant.now(), Duration.ofSeconds(Optional.ofNullable(beam.getDefaultTtl()).orElse(0)));
    }

    @Override
    public BeamFuture writeMessageToBeamAsync(String beamName,
                                              String messageKey,
                                              Object payload,
                                              Duration ttl) {
        PhotonBeam beam = getPhotonBeam(beamName, ttl.getSeconds());
        return writeMessageToBeamAsync(beam, messageKey, payload, Instant.now(), ttl);
    }

    @Override
    public BeamFuture writeMessageToBeamAsync(String beamName,
                                              String messageKey,
                                              Object payload,
                                              Instant writeTime) {
        PhotonBeam beam = getPhotonBeam(beamName, null);
        return writeMessageToBeamAsync(beam, messageKey, payload, writeTime, Duration.ofSeconds(Optional.ofNullable(beam.getDefaultTtl()).orElse(0)));
    }

    @Override
    public BeamFuture writeMessageToBeamAsync(String beamName,
                                              String messageKey,
                                              Object payload,
                                              Instant writeTime,
                                              Duration ttl) {
        PhotonBeam beam = getPhotonBeam(beamName, ttl.getSeconds());
        return writeMessageToBeamAsync(beam, messageKey, payload, writeTime, ttl);
    }

    private BeamFuture writeMessageToBeamAsync(PhotonBeam beam,
                                               String messageKey,
                                               Object payload,
                                               Instant writeTime,
                                               Duration ttl) {
        return new DefaultBeamFuture(beamDataDao.putMessageAsync(
                new PhotonProducerMessage(beam.getBeamUuid(),
                        messageKey,
                        serializer.serialize(beam, payload),
                        writeTime),
                (int)ttl.getSeconds()));
    }

    @Override
    public BeamFuture deleteMessageFromBeamAsync(String beamName,
                                                 String messageKey,
                                                 Instant writeTime) {
        Queue<PhotonBeam> beams = beamCache.getBeamByName(beamName);
        if (beams.isEmpty()) {
            throw new BeamNotFoundException(String.format("Could not find beam {}", beamName));
        }
        List<WriteFuture> resultSetFutures = Lists.newArrayList();
        for(PhotonBeam beam : beams) {
            resultSetFutures.add(beamDataDao.deleteMessageAsync(beam.getBeamUuid(), writeTime, messageKey));
        }
        return new DefaultBeamFuture(resultSetFutures);
    }

    @Override
    public int getPartitionSize() {
        return beamDataDao.getPartitionSize();
    }

    @Override
    public void setPartitionSize(int partitionSize) {
        beamDataDao.setPartitionSize(partitionSize);
    }

    private synchronized PhotonBeam getPhotonBeam(String beamName,
                                                  Long defaultTtl) {

        PhotonBeam beam = Optional.ofNullable(beamCache.getBeamByName(beamName).peek()).orElseGet(() -> {
            PhotonBeam photonBeam = new PhotonBeam();
            photonBeam.setBeamName(beamName);
            photonBeam.setStartDate(Instant.now());
            photonBeam.setBeamUuid(UUID.randomUUID());
            photonBeam.setDefaultTtl(Optional.ofNullable(defaultTtl).map(Long::intValue).orElse(null));
            beamCache.putBeam(photonBeam);
            return photonBeam;
        });

        return beam;
    }
}
