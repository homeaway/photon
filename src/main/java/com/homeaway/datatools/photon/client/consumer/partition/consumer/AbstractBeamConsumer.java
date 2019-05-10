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
package com.homeaway.datatools.photon.client.consumer.partition.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.Lists;
import com.homeaway.datatools.photon.api.beam.PhotonMessage;
import com.homeaway.datatools.photon.client.cache.BeamCache;
import com.homeaway.datatools.photon.client.cache.DefaultProcessedRecordCacheEntry;
import com.homeaway.datatools.photon.client.cache.ProcessedRecordCache;
import com.homeaway.datatools.photon.client.cache.ProcessedRecordCacheEntry;
import com.homeaway.datatools.photon.client.consumer.iterator.PhotonRowSetIterator;
import com.homeaway.datatools.photon.client.consumer.iterator.StatefulIterator;
import com.homeaway.datatools.photon.client.consumer.partition.producer.PartitionManifestResult;
import com.homeaway.datatools.photon.client.producer.BeamNotFoundException;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;
import com.homeaway.datatools.photon.dao.rows.PhotonRow;
import com.homeaway.datatools.photon.dao.rows.PhotonRowSetFuture;
import com.homeaway.datatools.photon.dao.beam.BeamDataManifestDao;
import com.homeaway.datatools.photon.dao.beam.BeamProcessedDao;
import com.homeaway.datatools.photon.dao.beam.BeamDataDao;
import com.homeaway.datatools.photon.serialization.PhotonDeserializer;
import com.homeaway.datatools.photon.utils.client.ConsumerUtils;
import com.homeaway.datatools.photon.utils.client.QueuedMessagesResult;
import com.homeaway.datatools.photon.utils.consumer.PartitionConsumer;
import static com.homeaway.datatools.photon.utils.dao.Constants.MESSAGE_KEY_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.PARTITION_TIME_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.PAYLOAD_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.WRITE_TIME_COLUMN;
import com.homeaway.datatools.photon.utils.dao.PartitionHelper;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Slf4j
abstract class AbstractBeamConsumer {

    private final PhotonDeserializer photonDeserializer;
    protected final BeamCache beamCache;
    protected final BeamDataManifestDao beamDataManifestDao;
    protected final PartitionHelper partitionHelper;
    final BeamDataDao beamDataDao;
    final BeamProcessedDao beamProcessedDao;
    final ProcessedRecordCache processedRecordCache;
    final PartitionConsumer<PartitionManifestResult, PhotonBeamReader> masterManifestConsumer;
    final PartitionConsumer<PartitionManifestResult, PhotonBeamReader> partitionManifestConsumer;
    final PartitionConsumer<QueuedMessagesResult, PhotonBeamReader> partitionConsumer;


    AbstractBeamConsumer(final PhotonDeserializer photonDeserializer,
                         final BeamCache beamCache,
                         final BeamDataDao beamDataDao,
                         final BeamDataManifestDao beamDataManifestDao,
                         final BeamProcessedDao beamProcessedDao,
                         final ProcessedRecordCache processedRecordCache,
                         final PartitionHelper partitionHelper,
                         final PartitionConsumer<PartitionManifestResult, PhotonBeamReader> masterManifestConsumer,
                         final PartitionConsumer<PartitionManifestResult, PhotonBeamReader> partitionManifestConsumer,
                         final PartitionConsumer<QueuedMessagesResult, PhotonBeamReader> partitionConsumer) {
        this.photonDeserializer = photonDeserializer;
        this.beamCache = beamCache;
        this.beamDataDao = beamDataDao;
        this.beamDataManifestDao = beamDataManifestDao;
        this.beamProcessedDao = beamProcessedDao;
        this.processedRecordCache = processedRecordCache;
        this.partitionHelper = partitionHelper;
        this.masterManifestConsumer = masterManifestConsumer;
        this.partitionManifestConsumer = partitionManifestConsumer;
        this.partitionConsumer = partitionConsumer;
    }

    ProcessedRecordCacheEntry translateProcessedResultSetToSet(UUID beamReaderUuid, Instant partitionTime, PhotonRowSetFuture photonRowSetFuture) {
        for (PhotonRow row : ConsumerUtils.GET_PHOTON_ROWSET_FROM_FUTURE.apply(photonRowSetFuture)) {
            processedRecordCache.putEntry(beamReaderUuid, partitionTime, row.getInstant(WRITE_TIME_COLUMN), row.getString(MESSAGE_KEY_COLUMN), false);
        }
        return processedRecordCache.getProcessedEntities(beamReaderUuid, partitionTime).orElse(new DefaultProcessedRecordCacheEntry());
    }

    PhotonMessage getPhotonMessage(PhotonBeamReader beamReader, PhotonRow row) {
        return beamCache.getBeamByUuid(beamReader.getBeamUuid())
                .map(b -> new DefaultPhotonMessage(photonDeserializer,
                            b,
                            beamReader.getBeamReaderUuid(),
                            row.getInstant(PARTITION_TIME_COLUMN),
                            row.getInstant(WRITE_TIME_COLUMN),
                            row.getString(MESSAGE_KEY_COLUMN),
                            row.getBytes(PAYLOAD_COLUMN))
                ).orElseThrow(() -> new BeamNotFoundException("Could not find beam"));
    }

    static PhotonMessage getEmptyPhotonMessage(Instant writeTime) {
        return new DefaultPhotonMessage(null, null, UUID.randomUUID(), writeTime);
    }

    private static class DefaultPhotonMessage implements PhotonMessage {

        private static final ObjectMapper mapper = new ObjectMapper()
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .registerModule(new Jdk8Module())
                .registerModule(new JavaTimeModule());

        private final PhotonDeserializer photonDeserializer;
        private final PhotonBeam photonBeam;
        private final UUID beamReaderUuid;
        private final Instant partitionTime;
        private final Instant writeTime;
        private final String messageKey;
        private final byte[] payload;

        DefaultPhotonMessage(final PhotonDeserializer photonDeserializer,
                             final PhotonBeam photonBeam,
                             final UUID beamReaderUuid,
                             final Instant writeTime) {
            this(photonDeserializer, photonBeam, beamReaderUuid, writeTime, writeTime, UUID.randomUUID().toString(), null);
        }

        DefaultPhotonMessage(final PhotonDeserializer photonDeserializer,
                             final PhotonBeam photonBeam,
                             final UUID beamReaderUuid,
                             final Instant partitionTime,
                             final Instant writeTime,
                             final String messageKey,
                             final byte[] payload) {
            this.photonDeserializer = photonDeserializer;
            this.photonBeam = photonBeam;
            this.beamReaderUuid = beamReaderUuid;
            this.partitionTime = partitionTime;
            this.writeTime = writeTime;
            this.messageKey = messageKey;
            this.payload = payload;
        }

        @Override
        public UUID getBeamReaderUuid() {
            return beamReaderUuid;
        }

        @Override
        public Instant getPartitionTime() {
            return partitionTime;
        }

        @Override
        public Instant getWriteTime() {
            return writeTime;
        }

        @Override
        public String getMessageKey() {
            return messageKey;
        }

        @Override
        public <T> T getPayload(Class<T> clazz) {
            return photonDeserializer.deserialize(photonBeam, payload, clazz);
        }

        @Override
        public String getPayloadString() {
            try {
                return mapper.writeValueAsString(getPayload(Object.class));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public byte[] getPayloadBytes() {
            return payload;
        }
    }

    public void stopConsumers(UUID key) {
        partitionConsumer.shutDownProducer(key);
        partitionManifestConsumer.shutDownProducer(key);
    }

    List<StatefulIterator<PhotonRow>> getStatefulIterators(List<PhotonRowSetFuture> futures) {
        List<StatefulIterator<PhotonRow>> iterators = Lists.newArrayList();
        for(PhotonRowSetFuture f : futures) {
            iterators.add(new PhotonRowSetIterator(ConsumerUtils.GET_PHOTON_ROWSET_FROM_FUTURE.apply(f)));
        }
        return iterators;
    }
}
