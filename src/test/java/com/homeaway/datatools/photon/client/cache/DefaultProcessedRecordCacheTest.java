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

import com.google.common.collect.Lists;
import com.homeaway.datatools.photon.api.beam.PhotonMessage;
import com.homeaway.datatools.photon.PhotonClientMockHelper;
import com.homeaway.datatools.photon.client.PhotonClientTestHelper;
import com.homeaway.datatools.photon.dao.beam.BeamProcessedDao;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;
import static com.homeaway.datatools.photon.utils.client.ClientConstants.PARTITION_SIZE_MILLISECONDS;
import static com.homeaway.datatools.photon.utils.client.ConsumerUtils.GET_PHOTON_ROWSET_FROM_FUTURE;
import com.homeaway.datatools.photon.utils.dao.DefaultPartitionHelper;
import com.homeaway.datatools.photon.utils.dao.PartitionHelper;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class DefaultProcessedRecordCacheTest {

    private PartitionHelper partitionHelper;
    private ProcessedRecordCache processedRecordCache;
    private BeamProcessedDao beamProcessedDao;
    private PhotonBeamReader photonBeamReader;

    @Before
    public void init() {
        partitionHelper = new DefaultPartitionHelper(PARTITION_SIZE_MILLISECONDS);
        beamProcessedDao = PhotonClientMockHelper.mockBeamProcessedDao();
        processedRecordCache = new DefaultProcessedRecordCache(beamProcessedDao);
        photonBeamReader = PhotonClientTestHelper.buildPhotonBeamReader();
        photonBeamReader.setWaterMark(Instant.now());
    }

    @Test
    public void putGetEntryTest() {
        Instant writeTime = Instant.now();
        PhotonMessage message = new TestPhotonMessage(partitionHelper, photonBeamReader, writeTime, "TestMessageKey");
        processedRecordCache.putEntry(message, TRUE);
        Optional<ProcessedRecordCacheEntry> processedRecordCacheEntry = processedRecordCache.getProcessedEntities(photonBeamReader, partitionHelper.getPartitionKey(message.getWriteTime()));
        Assert.assertTrue(processedRecordCacheEntry.isPresent());
        Assert.assertEquals(1, processedRecordCacheEntry.get().size());
        Assert.assertEquals(1, GET_PHOTON_ROWSET_FROM_FUTURE
                .apply(beamProcessedDao.getProcessedMessages(photonBeamReader.getBeamReaderUuid(),
                        partitionHelper.getPartitionKey(writeTime))).getSize());
    }

    @Test
    public void putGetMultipleEntryTest() {
        Instant writeTime = Instant.now();
        List<String> messageKeys = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            PhotonMessage message = new TestPhotonMessage(partitionHelper, photonBeamReader, writeTime,
                    String.format("TestMessageKey-%s", UUID.randomUUID()));
            processedRecordCache.putEntry(message, TRUE);
            messageKeys.add(message.getMessageKey());
        }
        Optional<ProcessedRecordCacheEntry> processedRecordCacheEntry = processedRecordCache.getProcessedEntities(photonBeamReader, partitionHelper.getPartitionKey(writeTime));
        Assert.assertTrue(processedRecordCacheEntry.isPresent());
        Assert.assertEquals(10, processedRecordCacheEntry.get().size());
        Assert.assertEquals(10, GET_PHOTON_ROWSET_FROM_FUTURE
                .apply(beamProcessedDao.getProcessedMessages(photonBeamReader.getBeamReaderUuid(),
                        partitionHelper.getPartitionKey(writeTime))).getSize());
        for (String k : messageKeys) {
            Assert.assertTrue(processedRecordCacheEntry.get().isPresent(writeTime, k));
        }
    }

    @Test
    public void putGetEntryTestNotPersisted() {
        Instant writeTime = Instant.now();
        PhotonMessage message = new TestPhotonMessage(partitionHelper, photonBeamReader, writeTime, "TestMessageKey");
        processedRecordCache.putEntry(message, FALSE);
        Optional<ProcessedRecordCacheEntry> processedRecordCacheEntry = processedRecordCache.getProcessedEntities(photonBeamReader, partitionHelper.getPartitionKey(message.getWriteTime()));
        Assert.assertTrue(processedRecordCacheEntry.isPresent());
        Assert.assertEquals(1, processedRecordCacheEntry.get().size());
        Assert.assertEquals(0, GET_PHOTON_ROWSET_FROM_FUTURE
                .apply(beamProcessedDao.getProcessedMessages(photonBeamReader.getBeamReaderUuid(),
                        partitionHelper.getPartitionKey(writeTime))).getSize());
    }

    @Test
    public void putGetMultipleEntryTestNotPersisted() {
        Instant writeTime = Instant.now();
        List<String> messageKeys = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            PhotonMessage message = new TestPhotonMessage(partitionHelper, photonBeamReader, writeTime,
                    String.format("TestMessageKey-%s", UUID.randomUUID()));
            processedRecordCache.putEntry(message, FALSE);
            messageKeys.add(message.getMessageKey());
        }
        Optional<ProcessedRecordCacheEntry> processedRecordCacheEntry = processedRecordCache.getProcessedEntities(photonBeamReader, partitionHelper.getPartitionKey(writeTime));
        Assert.assertTrue(processedRecordCacheEntry.isPresent());
        Assert.assertEquals(10, processedRecordCacheEntry.get().size());
        Assert.assertEquals(0, GET_PHOTON_ROWSET_FROM_FUTURE
                .apply(beamProcessedDao.getProcessedMessages(photonBeamReader.getBeamReaderUuid(),
                        partitionHelper.getPartitionKey(writeTime))).getSize());
        for (String k : messageKeys) {
            Assert.assertTrue(processedRecordCacheEntry.get().isPresent(writeTime, k));
        }
    }

    private static class TestPhotonMessage implements PhotonMessage {

        private final PartitionHelper partitionHelper;
        private final PhotonBeamReader photonBeamReader;
        private final Instant writeTime;
        private final String messageKey;

        public TestPhotonMessage(final PartitionHelper partitionHelper,
                                 final PhotonBeamReader photonBeamReader,
                                 final Instant writeTime,
                                 final String messageKey) {
            this.partitionHelper = partitionHelper;
            this.photonBeamReader = photonBeamReader;
            this.writeTime = writeTime;
            this.messageKey = messageKey;
        }

        @Override
        public UUID getBeamReaderUuid() {
            return photonBeamReader.getBeamReaderUuid();
        }

        @Override
        public Instant getPartitionTime() {
            return partitionHelper.getPartitionKey(writeTime);
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
            return clazz.cast(UUID.randomUUID());
        }

        @Override
        public String getPayloadString() {
            return UUID.randomUUID().toString();
        }

        @Override
        public byte[] getPayloadBytes() {
            return UUID.randomUUID().toString().getBytes();
        }
    }
}
