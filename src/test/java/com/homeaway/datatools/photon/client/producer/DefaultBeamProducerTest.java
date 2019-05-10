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
import com.google.common.collect.Maps;
import com.homeaway.datatools.photon.api.beam.BeamFuture;
import com.homeaway.datatools.photon.api.beam.BeamProducer;
import com.homeaway.datatools.photon.api.schema.SchemaClient;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamDao;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamDataDao;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamManifestDao;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamSchemaDao;
import com.homeaway.datatools.photon.client.cache.BeamCache;
import com.homeaway.datatools.photon.client.cache.DefaultBeamCache;
import com.homeaway.datatools.photon.client.schema.LocalSchemaClient;
import static com.homeaway.datatools.photon.client.schema.LocalSchemaClient.BEAM_SCHEMA_DAO;
import com.homeaway.datatools.photon.dao.beam.BeamDataDao;
import com.homeaway.datatools.photon.dao.beam.BeamDataManifestDao;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import com.homeaway.datatools.photon.dao.model.message.PhotonProducerMessage;
import com.homeaway.datatools.photon.dao.rows.PhotonRowSet;
import com.homeaway.datatools.photon.dao.rows.PhotonRowSetFuture;
import com.homeaway.datatools.photon.serialization.AvroPhotonSerializer;
import static com.homeaway.datatools.photon.serialization.AvroPhotonSerializer.AVRO_SCHEMA_CLIENT;
import com.homeaway.datatools.photon.serialization.JsonPhotonSerializer;
import com.homeaway.datatools.photon.serialization.PhotonSerializer;
import static com.homeaway.datatools.photon.utils.client.ConsumerUtils.GET_PHOTON_ROWSET_FROM_FUTURE;
import com.homeaway.datatools.photon.utils.dao.DefaultPartitionHelper;
import com.homeaway.datatools.photon.utils.dao.PartitionHelper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

public class DefaultBeamProducerTest {

    private static final String BEAM_NAME = "test.beam.name";
    private static final String BEAM_NAME_TTL = "test.beam.name.ttl";
    private BeamCache beamCache;
    private PartitionHelper partitionHelper;
    private BeamDataManifestDao beamDataManifestDao;
    private BeamDataDao beamDataDao;
    private SchemaClient schemaClient;

    @Before
    public void init() {
        beamCache = new DefaultBeamCache(mockBeamDao());
        partitionHelper = new DefaultPartitionHelper(100);
        beamDataManifestDao = mockBeamManifestDao(partitionHelper);
        beamDataDao = mockBeamDataDao(beamDataManifestDao, partitionHelper);
        Properties properties = new Properties();
        properties.put(BEAM_SCHEMA_DAO, mockBeamSchemaDao());
        schemaClient = new LocalSchemaClient();
        schemaClient.configure(properties);
    }

    @Test (expected = RuntimeException.class)
    public void schemaMismatchTest() {
        String messageKey = "TestMessageKey1";
        Map<String, Object> serializerConfig = Maps.newHashMap();
        serializerConfig.put(AVRO_SCHEMA_CLIENT, schemaClient);
        PhotonSerializer serializer = new AvroPhotonSerializer();
        serializer.configure(serializerConfig);
        BeamProducer beamProducer = new DefaultBeamProducer(beamDataDao, beamCache, serializer);
        beamProducer.writeMessageToBeam(BEAM_NAME, messageKey, UUID.randomUUID());
        beamProducer.writeMessageToBeam(BEAM_NAME, messageKey, new PhotonBeam());
    }

    @Test (expected = BeamNotFoundException.class)
    public void beamNotFoundTest() {
        String messageKey = "TestMessageKey1";
        BeamProducer beamProducer = new DefaultBeamProducer(beamDataDao, beamCache, new JsonPhotonSerializer());
        beamProducer.deleteMessageFromBeam(BEAM_NAME, messageKey, Instant.now());
    }

    @Test
    public void writeMessageToBeamTest() {
        String messageKey = "TestMessageKey1";
        Instant now = Instant.now().minusMillis(100);
        BeamProducer beamProducer = new DefaultBeamProducer(beamDataDao, beamCache, new JsonPhotonSerializer());
        beamProducer.writeMessageToBeam(BEAM_NAME, messageKey, UUID.randomUUID());
        PhotonBeam beam = beamCache.getBeamByName(BEAM_NAME).peek();

        testAssertions(now, beam);
    }

    @Test(expected = RuntimeException.class)
    public void writeMessageToBeamFailureTest() {
        String messageKey = "TestMessageKey1";
        BeamDataDao beamDataDao = mock(BeamDataDao.class);
        doThrow(RuntimeException.class).when(beamDataDao).putMessageAsync(any(PhotonProducerMessage.class), anyInt());
        BeamProducer beamProducer = new DefaultBeamProducer(beamDataDao, beamCache, new JsonPhotonSerializer());
        beamProducer.writeMessageToBeam(BEAM_NAME, messageKey, UUID.randomUUID());
    }

    @Test
    public void writeMessageToBeamTestFuture() {
        String messageKey = "TestMessageKey1";
        Instant future = Instant.now().plusSeconds(10);
        BeamProducer beamProducer = new DefaultBeamProducer(beamDataDao, beamCache, new JsonPhotonSerializer());
        beamProducer.writeMessageToBeam(BEAM_NAME, messageKey, UUID.randomUUID(), future);

        testAssertions(future, beamCache.getBeamByName(BEAM_NAME).peek());
    }

    @Test
    public void writeMessageToBeamWithTtlTest() {
        String messageKey = "TestMessageKey1";
        Instant now = Instant.now();
        BeamProducer beamProducer = new DefaultBeamProducer(beamDataDao, beamCache, new JsonPhotonSerializer());
        beamProducer.writeMessageToBeam(BEAM_NAME_TTL, messageKey, UUID.randomUUID(), Duration.ofSeconds(1L));
        PhotonBeam beam = beamCache.getBeamByName(BEAM_NAME_TTL).peek();
        testAssertionsWithTtl(now, beam, 1);
    }

    @Test
    public void writeMessageToBeamWithCustomTtlTest() {
        Instant now = Instant.now();
        String messageKey = "TestMessageKey1";
        BeamProducer beamProducer = new DefaultBeamProducer(beamDataDao, beamCache, new JsonPhotonSerializer());

        beamProducer.writeMessageToBeam(BEAM_NAME_TTL, messageKey, UUID.randomUUID(), Duration.ofSeconds(1L));
        PhotonBeam beam = beamCache.getBeamByName(BEAM_NAME_TTL).peek();
        testAssertionsWithTtl(now, beam, 1);

        now = Instant.now();
        beamProducer.writeMessageToBeam(BEAM_NAME_TTL, messageKey, UUID.randomUUID(), Duration.ofSeconds(4L));
        testAssertionsWithTtl(now, beam, 4);
    }

    @Test
    public void writeMessageToBeamWithDefaultTtlTest() {
        String messageKey = "TestMessageKey1";
        Instant now = Instant.now();

        BeamProducer beamProducer = new DefaultBeamProducer(beamDataDao, beamCache, new JsonPhotonSerializer());
        beamProducer.writeMessageToBeam(BEAM_NAME_TTL, messageKey, UUID.randomUUID(), Duration.ofSeconds(1L));
        PhotonBeam beam = beamCache.getBeamByName(BEAM_NAME_TTL).peek();
        testAssertionsWithTtl(now, beam, 1);

        now = Instant.now();
        beamProducer.writeMessageToBeam(BEAM_NAME_TTL, messageKey, UUID.randomUUID());
        testAssertionsWithTtl(now, beam, 1);

    }

    @Test
    public void writeMessageToBeamWithTtlTestFuture() {
        String messageKey = "TestMessageKey1";
        Instant future = Instant.now().plusSeconds(10);
        BeamProducer beamProducer = new DefaultBeamProducer(beamDataDao, beamCache, new JsonPhotonSerializer());
        beamProducer.writeMessageToBeam(BEAM_NAME_TTL, messageKey, UUID.randomUUID(), future, Duration.ofSeconds(1L));
        PhotonBeam beam = beamCache.getBeamByName(BEAM_NAME_TTL).peek();
        testAssertionsWithTtl(future, beam, 1);
        Assert.assertEquals(1, beam.getDefaultTtl().intValue());
    }

    @Test
    public void deleteMessageFromBeamTest() {
        String messageKey = "TestMessageKey1";
        Instant future = Instant.now().plusSeconds(10);
        BeamProducer beamProducer = new DefaultBeamProducer(beamDataDao, beamCache, new JsonPhotonSerializer());
        beamProducer.writeMessageToBeam(BEAM_NAME, messageKey, UUID.randomUUID(), future);
        PhotonBeam beam = beamCache.getBeamByName(BEAM_NAME).peek();
        PhotonRowSet photonRowSet = GET_PHOTON_ROWSET_FROM_FUTURE.apply(beamDataDao.getQueuePartitionAsync(beam.getBeamUuid(), partitionHelper.getPartitionKey(future)));
        Assert.assertEquals(1, photonRowSet.getSize());

        beamProducer.deleteMessageFromBeam(BEAM_NAME, messageKey, future);
        photonRowSet = GET_PHOTON_ROWSET_FROM_FUTURE.apply(beamDataDao.getQueuePartitionAsync(beam.getBeamUuid(), partitionHelper.getPartitionKey(future)));
        Assert.assertEquals(0, photonRowSet.getSize());
    }

    @Test
    public void writeMessageToBeamAsyncTest() {
        String messageKey = "TestMessageKey1";
        Instant now = Instant.now();
        BeamProducer beamProducer = new DefaultBeamProducer(beamDataDao, beamCache, new JsonPhotonSerializer());
        BeamFuture beamFuture = beamProducer.writeMessageToBeamAsync(BEAM_NAME, messageKey, UUID.randomUUID());
        Assert.assertNotNull(beamFuture);
        testAssertions(now, beamCache.getBeamByName(BEAM_NAME).peek());
    }

    @Test
    public void writeMessageToBeamAsyncTtlTest() {
        String messageKey = "TestMessageKey1";
        Instant now = Instant.now();
        BeamProducer beamProducer = new DefaultBeamProducer(beamDataDao, beamCache, new JsonPhotonSerializer());
        BeamFuture beamFuture = beamProducer.writeMessageToBeamAsync(BEAM_NAME_TTL, messageKey, UUID.randomUUID(), Duration.ofSeconds(1L));
        PhotonBeam beam = beamCache.getBeamByName(BEAM_NAME_TTL).peek();
        Assert.assertNotNull(beamFuture);
        Assert.assertEquals(1, beam.getDefaultTtl().intValue());
        testAssertionsWithTtl(now, beam, 1);
    }

    @Test
    public void writeMessageToBeamAsyncTestFuture() {
        String messageKey = "TestMessageKey1";
        Instant future = Instant.now().plusSeconds(10);
        BeamProducer beamProducer = new DefaultBeamProducer(beamDataDao, beamCache, new JsonPhotonSerializer());
        BeamFuture beamFuture = beamProducer.writeMessageToBeamAsync(BEAM_NAME, messageKey, UUID.randomUUID(), future);
        Assert.assertNotNull(beamFuture);
        testAssertions(future, beamCache.getBeamByName(BEAM_NAME).peek());
    }

    @Test
    public void writeMessageToBeamAsyncTtlTestFuture() {
        String messageKey = "TestMessageKey1";
        Instant future = Instant.now().plusSeconds(10);
        BeamProducer beamProducer = new DefaultBeamProducer(beamDataDao, beamCache, new JsonPhotonSerializer());
        BeamFuture beamFuture = beamProducer.writeMessageToBeamAsync(BEAM_NAME_TTL, messageKey, UUID.randomUUID(), future, Duration.ofSeconds(1L));
        PhotonBeam beam = beamCache.getBeamByName(BEAM_NAME_TTL).peek();
        Assert.assertNotNull(beamFuture);
        testAssertionsWithTtl(future, beam, 1);
        Assert.assertEquals(1, beam.getDefaultTtl().intValue());
    }

    @Test
    public void deleteMessageFromBeamAsyncTest() {
        BeamFuture beamFuture;
        String messageKey = "TestMessageKey1";
        Instant future = Instant.now().plusSeconds(10);
        BeamProducer beamProducer = new DefaultBeamProducer(beamDataDao, beamCache, new JsonPhotonSerializer());
        beamFuture = beamProducer.writeMessageToBeamAsync(BEAM_NAME, messageKey, UUID.randomUUID(), future);
        PhotonBeam beam = beamCache.getBeamByName(BEAM_NAME).peek();
        Assert.assertNotNull(beamFuture);
        testAssertions(future, beam);

        beamFuture = beamProducer.deleteMessageFromBeamAsync(BEAM_NAME, messageKey, future);
        Assert.assertNotNull(beamFuture);

        PhotonRowSet photonRowSet = GET_PHOTON_ROWSET_FROM_FUTURE.apply(beamDataDao.getQueuePartitionAsync(beam.getBeamUuid(), partitionHelper.getPartitionKey(future)));
        Assert.assertEquals(0, photonRowSet.getSize());

    }

    private void testAssertions(Instant startPartition, PhotonBeam beam) {
        List<PhotonRowSetFuture> futures = Lists.newArrayList();
        for (int i = 0; i < 5; i++) {
            futures.add(beamDataDao.getQueuePartitionAsync(beam.getBeamUuid(), partitionHelper.getPartitionKey(startPartition.plusMillis(i * 100))));
        }

        List<PhotonRowSet> photonRowSets = futures.stream().map(GET_PHOTON_ROWSET_FROM_FUTURE).collect(Collectors.toList());
        Assert.assertTrue(photonRowSets.stream()
                .anyMatch(rs -> rs.getSize() > 0));
    }

    private void testAssertionsWithTtl(Instant startPartition, PhotonBeam beam, Integer ttlValue) {
        List<PhotonRowSetFuture> futures = Lists.newArrayList();
        for (int i = 0; i < 5; i++) {
            futures.add(beamDataDao.getQueuePartitionAsync(beam.getBeamUuid(), partitionHelper.getPartitionKey(startPartition.plusMillis(i * 100))));
        }

        List<PhotonRowSet> photonRowSets = futures.stream().map(GET_PHOTON_ROWSET_FROM_FUTURE).collect(Collectors.toList());
        Assert.assertTrue(photonRowSets.stream()
                .anyMatch(rs -> rs.getSize() > 0));
        try {
            Thread.sleep(Duration.ofSeconds(ttlValue).toMillis());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        futures = Lists.newArrayList();
        for (int i = 0; i < 5; i++) {
            futures.add(beamDataDao.getQueuePartitionAsync(beam.getBeamUuid(), partitionHelper.getPartitionKey(startPartition.plusMillis(i * 100))));
        }

        photonRowSets = futures.stream().map(GET_PHOTON_ROWSET_FROM_FUTURE).collect(Collectors.toList());
        Assert.assertFalse(photonRowSets.stream()
                .anyMatch(rs -> rs.getSize() > 0));
    }
}
