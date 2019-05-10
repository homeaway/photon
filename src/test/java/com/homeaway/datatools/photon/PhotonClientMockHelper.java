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
package com.homeaway.datatools.photon;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.homeaway.datatools.photon.api.beam.PhotonMessageHandler;
import com.homeaway.datatools.photon.client.consumer.partition.consumer.BeamConsumer;
import com.homeaway.datatools.photon.dao.beam.BeamDao;
import com.homeaway.datatools.photon.dao.beam.BeamDataDao;
import com.homeaway.datatools.photon.dao.beam.BeamDataManifestDao;
import com.homeaway.datatools.photon.dao.beam.BeamProcessedDao;
import com.homeaway.datatools.photon.dao.beam.BeamReaderDao;
import com.homeaway.datatools.photon.dao.beam.BeamReaderLockDao;
import com.homeaway.datatools.photon.dao.beam.BeamSchemaDao;
import com.homeaway.datatools.photon.dao.beam.WriteFuture;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReaderLock;
import com.homeaway.datatools.photon.dao.model.message.PhotonProducerMessage;
import com.homeaway.datatools.photon.dao.rows.PhotonRow;
import com.homeaway.datatools.photon.dao.rows.PhotonRowSet;
import com.homeaway.datatools.photon.dao.rows.PhotonRowSetFuture;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_READER_UUID_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.BEAM_UUID_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.CLIENT_NAME_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.MANIFEST_TIME_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.MASTER_MANIFEST_TIME_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.MESSAGE_KEY_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.PARTITION_TIME_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.PAYLOAD_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.WATERMARK_COLUMN;
import static com.homeaway.datatools.photon.utils.dao.Constants.WRITE_TIME_COLUMN;
import com.homeaway.datatools.photon.utils.dao.PartitionHelper;
import static java.time.temporal.ChronoUnit.MILLIS;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

@Slf4j
public class PhotonClientMockHelper {

    public static PhotonRowSet mockPhotonRowSet(Queue<PhotonRow> rows) {
        return new MockPhotonRowSet(rows);
    }

    public static BeamDao mockBeamDao() {
        return new MockBeamDao();
    }

    public static BeamDao mockBeamDao(ConcurrentMap<String, Map<UUID, PhotonBeam>> beamsByName,
                                      ConcurrentMap<UUID, PhotonBeam> beamsByUuid) {
        return new MockBeamDao(beamsByName, beamsByUuid);
    }

    public static BeamReaderDao mockBeamReaderDao() {
        return new MockBeamReaderDao();
    }

    public static BeamReaderDao mockBeamReaderDao(Duration futureDelay) {
        return new MockBeamReaderDao(futureDelay);
    }

    public static BeamReaderDao mockBeamReaderDao(Long offset) {
        return new MockBeamReaderDao(offset);
    }

    public static BeamDataManifestDao mockBeamManifestDao(PartitionHelper partitionHelper,
                                                          Integer errorFrequency) {
        return new MockBeamDataManifestDao(partitionHelper, errorFrequency);
    }

    public static BeamDataManifestDao mockBeamManifestDao(PartitionHelper partitionHelper) {
        return new MockBeamDataManifestDao(partitionHelper);
    }

    public static BeamDataDao mockBeamDataDao(BeamDataManifestDao beamDataManifestDao,
                                       PartitionHelper partitionHelper) {
        return new MockBeamDataDao(beamDataManifestDao, partitionHelper);
    }

    public static BeamProcessedDao mockBeamProcessedDao() {
        return new MockBeamProcessedDao();
    }

    public static BeamConsumer mockBeamConsumer(BiConsumer<PhotonBeamReader, PhotonMessageHandler> task) {
        return new MockBeamConsumer(task);
    }

    public static BeamReaderLockDao mockBeamReaderLockDao(Duration lockThreshold) {
        return new MockBeamReaderLockDao(lockThreshold);
    }

    public static BeamReaderLockDao mockBeamReaderLockDao(Duration lockThreshold, Boolean immediateResponse) {
        return new MockBeamReaderLockDao(lockThreshold, immediateResponse);
    }

    public static BeamSchemaDao mockBeamSchemaDao() {
        return new MockBeamSchemaDao();
    }

    public static PhotonRow mockPhotonRow(Map<String, Object> values) {
        return new MockPhotonRow(values);
    }

    private static PhotonRowSetFuture getPhotonRowSetFuture(PhotonRowSet photonRows) {
        return new MockPhotonRowSetFuture(photonRows);
    }

    private static PhotonRowSetFuture getPhotonRowSetFutureException(PhotonRowSet photonRows) {
        return new MockPhotonRowSetFutureException(photonRows);
    }

    private static class MockBeamDao implements BeamDao {

        private final ConcurrentMap<String, Map<UUID, PhotonBeam>> beamsByName;
        private final ConcurrentMap<UUID, PhotonBeam> beamsByUuid;

        public MockBeamDao() {
            this(Maps.newConcurrentMap(), Maps.newConcurrentMap());
        }

        public MockBeamDao(final ConcurrentMap<String, Map<UUID, PhotonBeam>> beamsByName,
                           final ConcurrentMap<UUID, PhotonBeam> beamsByUuid) {
            this.beamsByName = beamsByName;
            this.beamsByUuid = beamsByUuid;
        }

        @Override
        public void putBeam(PhotonBeam photonBeam) {
            beamsByName.computeIfAbsent(photonBeam.getBeamName(), k -> Maps.newTreeMap())
                    .put(photonBeam.getBeamUuid(), photonBeam);
            beamsByUuid.put(photonBeam.getBeamUuid(), photonBeam);

        }

        @Override
        public Queue<PhotonBeam> getBeamByName(String beamName) {
            Queue<PhotonBeam> beams = Lists.newLinkedList();
            beamsByName.getOrDefault(beamName, Maps.newTreeMap())
                    .entrySet()
                    .forEach(e -> beams.add(e.getValue()));
            return beams;
        }

        @Override
        public Optional<PhotonBeam> getBeamByUuid(UUID beamUuid) {
            return Optional.ofNullable(beamsByUuid.get(beamUuid));
        }
    }

    private static class MockBeamReaderDao implements BeamReaderDao {

        private final ConcurrentMap<String, Map<UUID, PhotonRow>> beamReaders;
        private final Duration futureDelay;
        private final Long offset;

        public MockBeamReaderDao() {
            this(Maps.newConcurrentMap(), null, null);
        }

        public MockBeamReaderDao(Duration futureDelay) {
            this(Maps.newConcurrentMap(), futureDelay, null);
        }

        public MockBeamReaderDao(Long offset) {
            this(Maps.newConcurrentMap(), null, offset);
        }

        public MockBeamReaderDao(final ConcurrentMap<String, Map<UUID, PhotonRow>> beamReaders,
                                 final Duration futureDelay,
                                 final Long offset) {
            this.beamReaders = beamReaders;
            this.futureDelay = futureDelay;
            this.offset = offset;
        }

        @Override
        public void putBeamReader(PhotonBeamReader photonBeamReader) {
            beamReaders.computeIfAbsent(photonBeamReader.getClientName(), k -> Maps.newTreeMap())
                    .put(photonBeamReader.getBeamUuid(), rowFromBeamReader(photonBeamReader));
        }

        @Override
        public Optional<PhotonBeamReader> getBeamReaderByClientNameBeamUuid(String clientName, UUID beamUuid) {
            return Optional.ofNullable(beamReaders.getOrDefault(clientName, Maps.newTreeMap()).get(beamUuid))
                    .map(r -> {
                        PhotonBeamReader beamReader = new PhotonBeamReader();
                        beamReader.setClientName(r.getString(CLIENT_NAME_COLUMN));
                        beamReader.setBeamUuid(r.getUuid(BEAM_UUID_COLUMN));
                        beamReader.setBeamReaderUuid(r.getUuid(BEAM_READER_UUID_COLUMN));
                        return Optional.of(beamReader);
                    }).orElse(Optional.empty());
        }

        @Override
        public WriteFuture updateWatermark(PhotonBeamReader photonBeamReader) {
            beamReaders.computeIfAbsent(photonBeamReader.getClientName(), k -> Maps.newTreeMap())
                    .put(photonBeamReader.getBeamUuid(), rowFromBeamReader(photonBeamReader));

            return new MockWriteFuture(futureDelay);
        }

        @Override
        public Instant getCurrentClusterTime() {
            if (offset != null) {
                return Instant.now().plusMillis(offset);
            }
            return Instant.now();
        }

        @Override
        public Optional<Instant> getWatermark(PhotonBeamReader photonBeamReader) {
            return Optional.ofNullable(beamReaders.getOrDefault(photonBeamReader.getClientName(), Maps.newTreeMap())
                    .get(photonBeamReader.getBeamUuid()))
                    .map(r -> Optional.ofNullable(r.getInstant(WATERMARK_COLUMN)))
                    .orElse(Optional.empty());
        }

        private static PhotonRow rowFromBeamReader(PhotonBeamReader beamReader) {
            Map<String, Object> values = Maps.newHashMap();
            values.put(CLIENT_NAME_COLUMN, beamReader.getClientName());
            values.put(BEAM_UUID_COLUMN, beamReader.getBeamUuid());
            values.put(BEAM_READER_UUID_COLUMN, beamReader.getBeamReaderUuid());
            values.put(WATERMARK_COLUMN, beamReader.getWaterMark());
            return new MockPhotonRow(values);
        }
    }

    private static class MockBeamDataDao implements BeamDataDao {

        private final BeamDataManifestDao beamDataManifestDao;
        private final PartitionHelper partitionHelper;
        private final ConcurrentMap<UUID, Map<Instant, Map<Instant, Map<String, Pair<PhotonRow, Instant>>>>> queuePartitions;

        public MockBeamDataDao(final BeamDataManifestDao beamDataManifestDao,
                               final PartitionHelper partitionHelper) {
            this(beamDataManifestDao, partitionHelper, Maps.newConcurrentMap());
        }

        public MockBeamDataDao(final BeamDataManifestDao beamDataManifestDao,
                               final PartitionHelper partitionHelper,
                               final ConcurrentMap<UUID, Map<Instant, Map<Instant, Map<String, Pair<PhotonRow, Instant>>>>> queuePartitions) {
            this.beamDataManifestDao = beamDataManifestDao;
            this.partitionHelper = partitionHelper;
            this.queuePartitions = queuePartitions;
        }

        @Override
        public int getPartitionSize() {
            return 0;
        }

        @Override
        public void setPartitionSize(int partitionSize) {

        }

        @Override
        public List<WriteFuture> putMessageAsync(PhotonProducerMessage photonProducerMessage, Integer messageTtl) {
            Instant expirationDate = Optional.ofNullable(messageTtl)
                    .map(t -> {
                        if (messageTtl > 0) {
                            return Instant.now().plusSeconds(messageTtl);
                        }
                        return null;
                    }).orElse(null);
            queuePartitions.computeIfAbsent(photonProducerMessage.getBeamUuid(), k -> Maps.newTreeMap())
                    .computeIfAbsent(partitionHelper.getPartitionKey(photonProducerMessage.getWriteTime()), k -> Maps.newTreeMap())
                    .computeIfAbsent(photonProducerMessage.getWriteTime(), k -> Maps.newTreeMap())
                    .put(photonProducerMessage.getMessageKey(), new ImmutablePair<>(getRowFromMessage(photonProducerMessage), expirationDate));

            WriteFuture masterManifestFuture = beamDataManifestDao.putBeamMasterManifest(photonProducerMessage.getBeamUuid(),
                    photonProducerMessage.getWriteTime(), messageTtl);
            WriteFuture manifestFuture = beamDataManifestDao.putBeamDataManifest(photonProducerMessage.getBeamUuid(), photonProducerMessage.getWriteTime(),
                    messageTtl);
            WriteFuture dataFuture = new MockWriteFuture();
            return Lists.newArrayList(dataFuture, manifestFuture, masterManifestFuture);
        }

        @Override
        public WriteFuture deleteMessageAsync(UUID beamUuid, Instant writeTime, String messageKey) {
            queuePartitions.getOrDefault(beamUuid, Maps.newTreeMap())
                    .getOrDefault(partitionHelper.getPartitionKey(writeTime), Maps.newTreeMap())
                    .getOrDefault(writeTime, Maps.newTreeMap())
                    .remove(messageKey);

            return new MockWriteFuture();
        }

        @Override
        public PhotonRowSetFuture getQueuePartitionAsync(UUID beamUuid, Instant partition) {
            PhotonRowSet photonRows = new MockPhotonRowSet(Lists.newLinkedList(queuePartitions.getOrDefault(beamUuid, Maps.newTreeMap())
                    .getOrDefault(partition, Maps.newTreeMap())
                    .entrySet()
                    .stream()
                    .map(e -> e.getValue().values()
                            .stream()
                            .filter(v -> v.getRight() == null || v.getRight().isAfter(Instant.now()))
                            .map(Pair::getLeft).collect(Collectors.toList()))
                    .collect(Collectors.toList())
                    .stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList())));
            return getPhotonRowSetFuture(photonRows);
        }

        private PhotonRow getRowFromMessage(PhotonProducerMessage photonProducerMessage) {
            Map<String, Object> values = Maps.newHashMap();
            values.put(BEAM_UUID_COLUMN, photonProducerMessage.getBeamUuid());
            values.put(MESSAGE_KEY_COLUMN, photonProducerMessage.getMessageKey());
            values.put(PAYLOAD_COLUMN, photonProducerMessage.getPayload());
            values.put(WRITE_TIME_COLUMN, photonProducerMessage.getWriteTime());
            values.put(PARTITION_TIME_COLUMN, partitionHelper.getPartitionKey(photonProducerMessage.getWriteTime()));
            return new MockPhotonRow(values);
        }
    }

    private static class MockBeamDataManifestDao implements BeamDataManifestDao {

        private final ConcurrentMap<UUID, Map<Instant, Map<Instant, PhotonRow>>> beamDataMasterManifests;
        private final ConcurrentMap<UUID, Map<Instant, Map<Instant, PhotonRow>>> beamDataManifests;
        private final ConcurrentMap<UUID, Map<Instant, PhotonRowSetFuture>> beamDataResultSetFutures;
        private final PartitionHelper partitionHelper;
        private final Integer errorFrequency;
        private final AtomicInteger callCounter;

        public MockBeamDataManifestDao(final PartitionHelper partitionHelper) {
            this(partitionHelper, null);
        }

        public MockBeamDataManifestDao(final PartitionHelper partitionHelper,
                                       final Integer errorFrequency) {
            this(partitionHelper, Maps.newConcurrentMap(), Maps.newConcurrentMap(), Maps.newConcurrentMap(), new AtomicInteger(0), errorFrequency);
        }

        public MockBeamDataManifestDao(final PartitionHelper partitionHelper,
                                       final ConcurrentMap<UUID, Map<Instant, Map<Instant, PhotonRow>>> beamDataMasterManifests,
                                       final ConcurrentMap<UUID, Map<Instant, Map<Instant, PhotonRow>>> beamDataManifests,
                                       final ConcurrentMap<UUID, Map<Instant, PhotonRowSetFuture>> beamDataResultSetFutures,
                                       final AtomicInteger callCounter,
                                       final Integer errorFrequency) {
            this.partitionHelper = partitionHelper;
            this.beamDataMasterManifests = beamDataMasterManifests;
            this.beamDataManifests = beamDataManifests;
            this.beamDataResultSetFutures = beamDataResultSetFutures;
            this.errorFrequency = errorFrequency;
            this.callCounter = callCounter;
        }

        @Override
        public PhotonRowSetFuture getBeamMasterManifest(UUID beamUuid, Instant masterManifestTime, Instant minManifestTime, Instant maxManifestTime) {
            return new MockPhotonRowSetFuture(new MockPhotonRowSet(
                    Lists.newLinkedList(beamDataMasterManifests.getOrDefault(beamUuid, Maps.newConcurrentMap())
                        .getOrDefault(masterManifestTime, Maps.newConcurrentMap())
                            .entrySet()
                            .stream()
                            .filter(e -> (e.getKey().isAfter(minManifestTime) || e.getKey().equals(minManifestTime))
                                    && (e.getKey().isBefore(maxManifestTime) || e.getKey().equals(maxManifestTime)))
                            .map(Map.Entry::getValue)
                            .collect(Collectors.toList()))));
        }

        @Override
        public WriteFuture putBeamMasterManifest(UUID beamUuid, Instant writeTime, Integer messageTtl) {
            Map<String, Object> values = Maps.newHashMap();
            values.put(BEAM_UUID_COLUMN, beamUuid);
            values.put(MASTER_MANIFEST_TIME_COLUMN, partitionHelper.getMasterManifestKey(writeTime));
            values.put(MANIFEST_TIME_COLUMN, partitionHelper.getManifestKey(writeTime));

            beamDataMasterManifests.computeIfAbsent(beamUuid, b -> Maps.newTreeMap())
                .computeIfAbsent(partitionHelper.getMasterManifestKey(writeTime), m -> Maps.newTreeMap())
                .put(partitionHelper.getManifestKey(writeTime), new MockPhotonRow(values));
            return new MockWriteFuture();
        }

        @Override
        public PhotonRowSetFuture getBeamDataManifest(UUID beamUuid, Instant manifestTime, Instant maxPartitionTime) {
            callCounter.incrementAndGet();
            PhotonRowSetFuture photonRowSetFuture = beamDataResultSetFutures.getOrDefault(beamUuid, Maps.newTreeMap())
                    .computeIfAbsent(manifestTime, k -> {
                        PhotonRowSet photonRows = new MockPhotonRowSet(Lists.newLinkedList(beamDataManifests.getOrDefault(beamUuid, Maps.newTreeMap())
                                .getOrDefault(manifestTime, Maps.newTreeMap())
                                .values()));

                        if (errorFrequency != null && callCounter.intValue() % errorFrequency == 0) {
                            return getPhotonRowSetFutureException(photonRows);
                        }
                        return getPhotonRowSetFuture(photonRows);
                    });
            return photonRowSetFuture;
        }

        @Override
        public WriteFuture putBeamDataManifest(UUID beamUuid, Instant writeTime, Integer messageTtl) {
            Map<String, Object> values = Maps.newHashMap();
            values.put(BEAM_UUID_COLUMN, beamUuid);
            values.put(MANIFEST_TIME_COLUMN, partitionHelper.getManifestKey(writeTime));
            values.put(PARTITION_TIME_COLUMN, partitionHelper.getPartitionKey(writeTime));
            PhotonRow photonRow = new MockPhotonRow(values);

            beamDataManifests.computeIfAbsent(beamUuid, k -> Maps.newTreeMap())
                    .computeIfAbsent(partitionHelper.getManifestKey(writeTime), k -> Maps.newTreeMap())
                    .put(partitionHelper.getPartitionKey(writeTime), photonRow);

            return new MockWriteFuture();
        }
    }

    private static class MockBeamProcessedDao implements BeamProcessedDao {

        private final ConcurrentMap<UUID, Map<Instant, Map<Instant, Set<String>>>> processedRecords;

        public MockBeamProcessedDao() {
            this(Maps.newConcurrentMap());
        }

        public MockBeamProcessedDao(final ConcurrentMap<UUID, Map<Instant, Map<Instant, Set<String>>>> processedRecords) {
            this.processedRecords = processedRecords;
        }

        @Override
        public WriteFuture putProcessedMessage(UUID photonBeamReaderUuid, Instant partitionTime, Instant writeTime, String messageKey) {
            processedRecords.computeIfAbsent(photonBeamReaderUuid, k -> Maps.newTreeMap())
                    .computeIfAbsent(partitionTime, k -> Maps.newTreeMap())
                    .computeIfAbsent(writeTime, k -> Sets.newConcurrentHashSet())
                    .add(messageKey);
            return new MockWriteFuture();
        }

        @Override
        public PhotonRowSetFuture getProcessedMessages(UUID photonBeamReaderUuid, Instant partitionTime) {
            PhotonRowSet photonRows = new MockPhotonRowSet(Lists.newLinkedList(processedRecords.getOrDefault(photonBeamReaderUuid, Maps.newTreeMap())
                    .getOrDefault(partitionTime, Maps.newTreeMap())
                    .entrySet()
                    .stream()
                    .map(e -> e.getValue().stream().map(v -> {
                                Map<String, Object> values = Maps.newHashMap();
                                values.put(BEAM_READER_UUID_COLUMN, photonBeamReaderUuid);
                                values.put(PARTITION_TIME_COLUMN, partitionTime);
                                values.put(WRITE_TIME_COLUMN, e.getKey());
                                values.put(MESSAGE_KEY_COLUMN, v);
                                return new MockPhotonRow(values);
                            })
                            .collect(Collectors.toList())
                        )
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList())));
            return getPhotonRowSetFuture(photonRows);
        }
    }

    private static class MockBeamConsumer implements BeamConsumer {

        private final BiConsumer<PhotonBeamReader, PhotonMessageHandler> task;

        public MockBeamConsumer(final BiConsumer<PhotonBeamReader, PhotonMessageHandler> task) {
            this.task = task;
        }

        @Override
        public void consume(PhotonBeamReader photonBeamReader, PhotonMessageHandler photonMessageHandler) {
            task.accept(photonBeamReader, photonMessageHandler);
        }

        @Override
        public void consume(PhotonBeamReader photonBeamReader, PhotonMessageHandler photonMessageHandler, Boolean isAsync) {
            task.accept(photonBeamReader, photonMessageHandler);
        }

        @Override
        public void stopConsumers(UUID key) {

        }
    }

    private static class MockBeamReaderLockDao implements BeamReaderLockDao {

        private final Duration lockThreshold;
        private final ConcurrentMap<String, ConcurrentMap<UUID, PhotonBeamReaderLock>> locks;
        private final Boolean immediateResponse;

        public MockBeamReaderLockDao(final Duration lockThreshold) {
            this(lockThreshold, Boolean.FALSE);
        }

        public MockBeamReaderLockDao(final Duration lockThreshold,
                                     final Boolean immediateResponse) {
            this(lockThreshold, Maps.newConcurrentMap(), immediateResponse);
        }

        public MockBeamReaderLockDao(final Duration lockThreshold,
                                     final ConcurrentMap<String, ConcurrentMap<UUID, PhotonBeamReaderLock>> locks,
                                     final Boolean immediateResponse) {
            this.lockThreshold = lockThreshold;
            this.locks = locks;
            this.immediateResponse = immediateResponse;
        }

        @Override
        public PhotonBeamReaderLock putBeamReaderLock(String clientName, UUID beamUuid, PhotonBeamReaderLock photonBeamReaderLock) {
            if (immediateResponse) {
                return photonBeamReaderLock;
            } else {
                locks.computeIfAbsent(clientName, k -> Maps.newConcurrentMap()).put(beamUuid, photonBeamReaderLock);
                try {
                    Thread.sleep(lockThreshold.dividedBy(4).toMillis());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return locks.get(clientName).get(beamUuid);
            }
        }

        @Override
        public void updateLock(PhotonBeamReader photonBeamReader) {
            locks.computeIfAbsent(photonBeamReader.getClientName(),
                    k -> Maps.newConcurrentMap()).put(photonBeamReader.getBeamUuid(), photonBeamReader.getPhotonBeamReaderLock().get());
        }

        @Override
        public Optional<PhotonBeamReaderLock> getAvailablePhotonBeamLock(PhotonBeamReader photonBeamReader) {
            Instant lockTimeThreshold = Instant.now().minus(lockThreshold.toMillis(), ChronoUnit.MILLIS);
            Optional<PhotonBeamReaderLock> lock = Optional.ofNullable(locks.computeIfAbsent(photonBeamReader.getClientName(),
                    k -> Maps.newConcurrentMap()).get(photonBeamReader.getBeamUuid()));

            return Optional.ofNullable(lock.map(l -> (l.getLockTime().isBefore(lockTimeThreshold) || immediateResponse) ? l : null)
                    .orElseGet(() -> {
                        PhotonBeamReaderLock newLock = new PhotonBeamReaderLock(UUID.randomUUID(), lockTimeThreshold);
                        locks.computeIfAbsent(photonBeamReader.getClientName(), k -> Maps.newConcurrentMap())
                                .put(photonBeamReader.getBeamUuid(), newLock);
                        return newLock;
                    }));
        }

        @Override
        public PhotonBeamReaderLock getPhotonBeamLock(PhotonBeamReader photonBeamReader) {
            return photonBeamReader.getPhotonBeamReaderLock().get();
        }
    }

    private static class MockBeamSchemaDao implements BeamSchemaDao {

        private final ConcurrentMap<UUID, Schema> schemas;

        public MockBeamSchemaDao() {
            this(Maps.newConcurrentMap());
        }

        public MockBeamSchemaDao(final ConcurrentMap<UUID, Schema> schemas) {
            this.schemas = schemas;
        }

        @Override
        public Optional<Schema> getSchemaByBeamUuid(UUID beamUuid) {
            return Optional.ofNullable(schemas.get(beamUuid));
        }

        @Override
        public WriteFuture putBeamSchema(PhotonBeam beam, Schema schema) {
            schemas.put(beam.getBeamUuid(), schema);
            return new MockWriteFuture();
        }
    }

    private static class MockPhotonRowSet implements PhotonRowSet {

        private final Queue<PhotonRow> rows;
        private final Iterator<PhotonRow> iterator;

        public MockPhotonRowSet(final Queue<PhotonRow> rows) {
            this.rows = rows;
            this.iterator = rows.iterator();
        }

        @Override
        public PhotonRow one() {
            return rows.poll();
        }

        @Override
        public int getSize() {
            return rows.size();
        }

        @Override
        public Iterator<PhotonRow> iterator() {
            return iterator;
        }
    }

    private static class MockWriteFuture implements WriteFuture {

        private final Instant createTime;
        private final Duration delay;

        public MockWriteFuture() {
            this(null);
        }

        public MockWriteFuture(final Duration delay) {
            this.createTime = Instant.now();
            this.delay = delay;
        }

        @Override
        public Boolean getUninterruptibly() {
            return Boolean.TRUE;
        }

        @Override
        public Boolean getUninterruptibly(long timeout, TimeUnit unit) throws TimeoutException {
            return Boolean.TRUE;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            Instant.now();
            if (delay != null) {
                long lapsed = createTime.until(Instant.now(), MILLIS);
                if (lapsed < delay.toMillis()) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public Boolean get() throws InterruptedException, ExecutionException {
            return Boolean.TRUE;
        }

        @Override
        public Boolean get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return Boolean.TRUE;
        }

        @Override
        public void addListener(Runnable listener, Executor executor) {
            executor.execute(listener);
        }
    }

    private static class MockPhotonRowSetFuture implements PhotonRowSetFuture {

        private final PhotonRowSet photonRows;

        public MockPhotonRowSetFuture(final PhotonRowSet photonRows) {
            this.photonRows = photonRows;
        }

        @Override
        public PhotonRowSet getUninterruptibly() {
            return photonRows;
        }

        @Override
        public PhotonRowSet getUninterruptibly(long timeout, TimeUnit unit) throws TimeoutException {
            return photonRows;
        }

        @Override
        public void addListener(Runnable listener, Executor executor) {

        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return true;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public PhotonRowSet get() throws InterruptedException, ExecutionException {
            return photonRows;
        }

        @Override
        public PhotonRowSet get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return photonRows;
        }
    }

    private static class MockPhotonRowSetFutureException implements PhotonRowSetFuture {

        private final PhotonRowSet photonRows;
        private final AtomicInteger getCounter;

        public MockPhotonRowSetFutureException(final PhotonRowSet photonRows) {
            this.photonRows = photonRows;
            this.getCounter = new AtomicInteger(0);
        }

        @Override
        public PhotonRowSet getUninterruptibly() {
            return photonRows;
        }

        @Override
        public PhotonRowSet getUninterruptibly(long timeout, TimeUnit unit) throws TimeoutException {
            if (getCounter.intValue() == 0) {
                getCounter.incrementAndGet();
                throw new TimeoutException();
            }
            return photonRows;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return true;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        public PhotonRowSet get() throws InterruptedException, ExecutionException {
            return photonRows;
        }

        @Override
        public PhotonRowSet get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return photonRows;
        }

        @Override
        public void addListener(Runnable listener, Executor executor) {

        }
    }

    private static class MockPhotonRow implements PhotonRow {

        private final Map<String, Object> values;

        public MockPhotonRow(final Map<String, Object> values) {
            this.values = values;
        }

        @Override
        public String getString(String columnName) {
            return values.get(columnName).toString();
        }

        @Override
        public Instant getInstant(String columnName) {
            return Optional.ofNullable(values.get(columnName))
                    .map(v -> (Instant) v)
                    .orElse(null);
        }

        @Override
        public UUID getUuid(String columnName) {
            return (UUID) values.get(columnName);
        }

        @Override
        public Integer getInt(String columnName) {
            return Optional.ofNullable(values.get(columnName)).map(v -> Integer.valueOf(v.toString())).orElse(null);
        }

        @Override
        public byte[] getBytes(String columnName) {
            return Optional.ofNullable(values.get(columnName)).map(v -> (byte[])v).orElse(null);
        }
    }
}
