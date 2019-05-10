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
package com.homeaway.datatools.photon.client.consumer.partition.producer;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import static com.homeaway.datatools.photon.client.PhotonClientTestHelper.buildPhotonBeam;
import static com.homeaway.datatools.photon.client.PhotonClientTestHelper.buildPhotonBeamReader;
import com.homeaway.datatools.photon.client.cache.BeamCache;
import static com.homeaway.datatools.photon.utils.client.ClientConstants.PARTITION_SIZE_MILLISECONDS;
import com.homeaway.datatools.photon.dao.beam.BeamDataManifestDao;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;
import static com.homeaway.datatools.photon.utils.client.PartitionProducerType.FORWARD;
import com.homeaway.datatools.photon.utils.consumer.BatchResult;
import com.homeaway.datatools.photon.utils.consumer.ConsumerQueue;
import com.homeaway.datatools.photon.utils.consumer.PartitionProducer;
import com.homeaway.datatools.photon.utils.dao.DefaultPartitionHelper;
import org.junit.Assert;
import org.junit.Test;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

public class MasterManifestPartitionProducerTest {

    @Test
    public void testProducePartitionResultsOneResult() {
        PhotonBeam photonBeam = buildPhotonBeam();
        PhotonBeamReader photonBeamReader = buildPhotonBeamReader();
        photonBeamReader.setBeamUuid(photonBeam.getBeamUuid());

        BeamCache beamCache = mock(BeamCache.class);
        when(beamCache.getBeamByName(any(String.class))).thenReturn(new LinkedList<>(Collections.singletonList(photonBeam)));
        when(beamCache.getBeamByUuid(any(UUID.class))).thenReturn(Optional.of(photonBeam));

        PartitionProducer<PartitionManifestResult, PhotonBeamReader> partitionProducer =
                new MasterManifestPartitionProducer(beamCache, mock(BeamDataManifestDao.class),
                        new DefaultPartitionHelper(PARTITION_SIZE_MILLISECONDS), FORWARD,
                        Instant.now(), Instant.now().plusSeconds(60));

        ConcurrentMap<UUID, ConsumerQueue<PartitionManifestResult>> consumerQueueMap = Maps.newConcurrentMap();
        consumerQueueMap.put(photonBeamReader.getBeamReaderUuid(), new ConsumerQueue<>(Queues.newArrayBlockingQueue(20)));

        ConcurrentMap<UUID, Boolean> producerRunnableFlags = Maps.newConcurrentMap();
        producerRunnableFlags.put(photonBeamReader.getBeamReaderUuid(), true);

        partitionProducer.producePartitionResults(photonBeamReader.getBeamReaderUuid(), consumerQueueMap, photonBeamReader,
                producerRunnableFlags);

        Assert.assertEquals(1, consumerQueueMap.get(photonBeamReader.getBeamReaderUuid()).getBatchResultQueue().size());
        BatchResult<PartitionManifestResult> result;
        do {
            result = consumerQueueMap.get(photonBeamReader.getBeamReaderUuid()).getBatchResultQueue().poll();
        } while (!result.isLastBatch());
        Assert.assertTrue(result.isLastBatch());
        Assert.assertTrue(consumerQueueMap.get(photonBeamReader.getBeamReaderUuid()).getBatchResultQueue().isEmpty());
    }

    @Test
    public void testProducePartitionResultsTwoResults() {
        PhotonBeam photonBeam = buildPhotonBeam();
        PhotonBeamReader photonBeamReader = buildPhotonBeamReader();
        photonBeamReader.setBeamUuid(photonBeam.getBeamUuid());

        BeamCache beamCache = mock(BeamCache.class);
        when(beamCache.getBeamByName(any(String.class))).thenReturn(new LinkedList<>(Collections.singletonList(photonBeam)));
        when(beamCache.getBeamByUuid(any(UUID.class))).thenReturn(Optional.of(photonBeam));

        PartitionProducer<PartitionManifestResult, PhotonBeamReader> partitionProducer =
                new MasterManifestPartitionProducer(beamCache, mock(BeamDataManifestDao.class),
                        new DefaultPartitionHelper(PARTITION_SIZE_MILLISECONDS), FORWARD,
                        Instant.now(), Instant.now().plus(7, ChronoUnit.HOURS));

        ConcurrentMap<UUID, ConsumerQueue<PartitionManifestResult>> consumerQueueMap = Maps.newConcurrentMap();
        consumerQueueMap.put(photonBeamReader.getBeamReaderUuid(), new ConsumerQueue<>(Queues.newArrayBlockingQueue(20)));

        ConcurrentMap<UUID, Boolean> producerRunnableFlags = Maps.newConcurrentMap();
        producerRunnableFlags.put(photonBeamReader.getBeamReaderUuid(), true);

        partitionProducer.producePartitionResults(photonBeamReader.getBeamReaderUuid(), consumerQueueMap, photonBeamReader,
                producerRunnableFlags);

        Assert.assertTrue(2 <= consumerQueueMap.get(photonBeamReader.getBeamReaderUuid()).getBatchResultQueue().size());
        BatchResult<PartitionManifestResult> result;
        do {
            result = consumerQueueMap.get(photonBeamReader.getBeamReaderUuid()).getBatchResultQueue().poll();
        } while (!result.isLastBatch());
        Assert.assertTrue(result.isLastBatch());
        Assert.assertTrue(consumerQueueMap.get(photonBeamReader.getBeamReaderUuid()).getBatchResultQueue().isEmpty());
    }

    @Test
    public void testShutDownProducer() {
        PhotonBeam photonBeam = buildPhotonBeam();
        PhotonBeamReader photonBeamReader = buildPhotonBeamReader();
        photonBeamReader.setBeamUuid(photonBeam.getBeamUuid());

        BeamCache beamCache = mock(BeamCache.class);
        when(beamCache.getBeamByName(any(String.class))).thenReturn(new LinkedList<>(Collections.singletonList(photonBeam)));
        when(beamCache.getBeamByUuid(any(UUID.class))).thenReturn(Optional.of(photonBeam));

        PartitionProducer<PartitionManifestResult, PhotonBeamReader> partitionProducer =
                new MasterManifestPartitionProducer(beamCache, mock(BeamDataManifestDao.class),
                        new DefaultPartitionHelper(PARTITION_SIZE_MILLISECONDS), FORWARD,
                        Instant.now(), Instant.now().plusSeconds(60));

        ConcurrentMap<UUID, ConsumerQueue<PartitionManifestResult>> consumerQueueMap = Maps.newConcurrentMap();
        consumerQueueMap.put(photonBeamReader.getBeamReaderUuid(), new ConsumerQueue<>(Queues.newArrayBlockingQueue(100)));

        ConcurrentMap<UUID, Boolean> producerRunnableFlags = Maps.newConcurrentMap();
        producerRunnableFlags.put(photonBeamReader.getBeamReaderUuid(), false);

        partitionProducer.producePartitionResults(photonBeamReader.getBeamReaderUuid(), consumerQueueMap, photonBeamReader,
                producerRunnableFlags);

        Assert.assertEquals(1, consumerQueueMap.get(photonBeamReader.getBeamReaderUuid()).getBatchResultQueue().size());
        BatchResult<PartitionManifestResult> result = consumerQueueMap.get(photonBeamReader.getBeamReaderUuid()).getBatchResultQueue().poll();
        Assert.assertTrue(result.isLastBatch());
        Assert.assertFalse(result.getResults().isPresent());
    }
}
