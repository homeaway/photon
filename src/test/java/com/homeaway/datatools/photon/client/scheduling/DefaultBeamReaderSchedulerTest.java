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
package com.homeaway.datatools.photon.client.scheduling;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.homeaway.datatools.photon.api.beam.PhotonMessageHandler;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamConsumer;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamDao;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamReaderDao;
import static com.homeaway.datatools.photon.client.PhotonClientTestHelper.buildPhotonBeam;
import static com.homeaway.datatools.photon.client.PhotonClientTestHelper.buildPhotonBeamReader;
import com.homeaway.datatools.photon.client.cache.BeamCache;
import com.homeaway.datatools.photon.client.cache.BeamReaderCache;
import com.homeaway.datatools.photon.client.cache.DefaultBeamCache;
import com.homeaway.datatools.photon.client.cache.DefaultBeamReaderCache;
import com.homeaway.datatools.photon.client.consumer.PhotonBeamReaderConfig;
import com.homeaway.datatools.photon.client.consumer.partition.consumer.BeamConsumer;
import com.homeaway.datatools.photon.dao.beam.BeamDao;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReaderLock;
import static com.homeaway.datatools.photon.api.model.enums.PhotonBeamReaderOffsetType.FROM_BEGINNING;
import static com.homeaway.datatools.photon.api.model.enums.PhotonBeamReaderOffsetType.FROM_CURRENT;
import com.homeaway.datatools.photon.utils.client.consumer.BeamReaderConfigManager;
import com.homeaway.datatools.photon.utils.client.consumer.DefaultBeamReaderConfigManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class DefaultBeamReaderSchedulerTest {

    private ConcurrentMap<PhotonBeamReader, List<Instant>> beamReaderRuns;
    private BeamCache beamCache;
    private BeamReaderCache beamReaderCache;
    private BeamReaderConfigManager beamReaderConfigManager;

    @Before
    public void init() {
        beamReaderRuns = Maps.newConcurrentMap();
        beamCache = new DefaultBeamCache(mockBeamDao());
        beamReaderCache = new DefaultBeamReaderCache(mockBeamReaderDao());
        beamReaderConfigManager = new DefaultBeamReaderConfigManager(beamCache, beamReaderCache);
    }

    @Test
    public void testFiveConfigsOneBeam() throws Exception {

        BeamConsumer beamConsumer = mockBeamConsumer((photonBeamReader, photonMessageHandler) ->
                beamReaderRuns.computeIfAbsent(photonBeamReader, k -> Lists.newArrayList()).add(Instant.now()));

        PhotonScheduler beamReaderScheduler = new DefaultBeamReaderScheduler(beamReaderConfigManager, beamCache, beamReaderCache, beamConsumer,
                (photonBeamReaderLockWrapper, photonMessageHandler, cache, configManager, consumer, isAsync) -> consumer.consume(photonBeamReaderLockWrapper.getPhotonBeamReader(), photonMessageHandler));

        String CLIENT_NAME = "TestClientName";

        PhotonBeam beam = buildPhotonBeam();
        beamCache.putBeam(beam);

        beamReaderConfigManager.putBeamReaderConfig(new PhotonBeamReaderConfig(CLIENT_NAME, beam.getBeamName(),
                mock(PhotonMessageHandler.class), FROM_BEGINNING, (clientName, beamName) -> Instant.now()));

        for(int i = 0; i < 4; i++) {
            beamReaderConfigManager.putBeamReaderConfig(new PhotonBeamReaderConfig(String.format("ClientName_%s",i),
                    beam.getBeamName(), mock(PhotonMessageHandler.class),
                    FROM_BEGINNING, (clientName, beamName) -> Instant.now()));
        }

        beamReaderCache.getPhotonBeamReader(CLIENT_NAME, beam).get().getPhotonBeamReader().setPhotonBeamReaderLock(new PhotonBeamReaderLock());
        beamReaderScheduler.setPollingInterval(Duration.ofMillis(10));
        beamReaderScheduler.start();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Assert.assertEquals(1, beamReaderRuns.size());
        Assert.assertTrue(0 < beamReaderRuns.get(beamReaderCache.getPhotonBeamReader(CLIENT_NAME, beam).get().getPhotonBeamReader()).size()
                && 11 >= beamReaderRuns.get(beamReaderCache.getPhotonBeamReader(CLIENT_NAME, beam).get().getPhotonBeamReader()).size());
        beamReaderScheduler.stop();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Assert.assertEquals(1, beamReaderRuns.size());
        Assert.assertTrue(10 <= beamReaderRuns.get(beamReaderCache.getPhotonBeamReader(CLIENT_NAME, beam).get().getPhotonBeamReader()).size()
                && 11 >= beamReaderRuns.get(beamReaderCache.getPhotonBeamReader(CLIENT_NAME, beam).get().getPhotonBeamReader()).size());
    }

    @Test
    public void testFiveConfigsSameBeam() throws Exception {

        BeamConsumer beamConsumer = mockBeamConsumer((photonBeamReader, photonMessageHandler) ->
                beamReaderRuns.computeIfAbsent(photonBeamReader, k -> Lists.newArrayList()).add(Instant.now()));

        PhotonScheduler beamReaderScheduler = new DefaultBeamReaderScheduler(beamReaderConfigManager, beamCache, beamReaderCache, beamConsumer,
                (photonBeamReaderLockWrapper, photonMessageHandler, cache, configManager, consumer, isAsync) -> consumer.consume(photonBeamReaderLockWrapper.getPhotonBeamReader(), photonMessageHandler));

        PhotonBeam beam = buildPhotonBeam();
        beamCache.putBeam(beam);

        for(int i = 0; i < 5; i++) {
            String client = String.format("ClientName_%s",i);
            beamReaderConfigManager.putBeamReaderConfig(new PhotonBeamReaderConfig(client, beam.getBeamName(),
                    mock(PhotonMessageHandler.class), FROM_BEGINNING, (clientName, beamName) -> Instant.now()));

            beamReaderCache.getPhotonBeamReader(client, beam).get().getPhotonBeamReader().setPhotonBeamReaderLock(new PhotonBeamReaderLock());
        }

        beamReaderScheduler.setPollingInterval(Duration.ofMillis(10));
        beamReaderScheduler.start();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        beamReaderScheduler.stop();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Assert.assertEquals(5, beamReaderRuns.keySet().size());
        Assert.assertTrue(0 < beamReaderRuns.values()
                .stream()
                .flatMap(List::stream)
                .collect(Collectors.toList()).size()
            && 55 >= beamReaderRuns.values()
                .stream()
                .flatMap(List::stream)
                .collect(Collectors.toList()).size());
    }

    @Test
    public void testFiveConfigsFiveBeams() throws Exception {

        BeamConsumer beamConsumer = mockBeamConsumer((photonBeamReader, photonMessageHandler) ->
                beamReaderRuns.computeIfAbsent(photonBeamReader, k -> Lists.newArrayList()).add(Instant.now()));

        PhotonScheduler beamReaderScheduler = new DefaultBeamReaderScheduler(beamReaderConfigManager, beamCache, beamReaderCache, beamConsumer,
                (photonBeamReaderLockWrapper, photonMessageHandler, cache, configManager, consumer, isAsync) -> consumer.consume(photonBeamReaderLockWrapper.getPhotonBeamReader(), photonMessageHandler));

        for(int i = 0; i < 5; i++) {
            String client = String.format("ClientName_%s",i);
            String beam = String.format("BeamName_%s", i);

            PhotonBeam photonBeam = buildPhotonBeam();
            photonBeam.setBeamName(beam);
            beamCache.putBeam(photonBeam);

            beamReaderConfigManager.putBeamReaderConfig(new PhotonBeamReaderConfig(client, beam,
                    mock(PhotonMessageHandler.class), FROM_BEGINNING, (clientName, beamName) -> Instant.now()));

            beamReaderCache.getPhotonBeamReader(client, photonBeam).get().getPhotonBeamReader().setPhotonBeamReaderLock(new PhotonBeamReaderLock());
        }

        beamReaderScheduler.setPollingInterval(Duration.ofMillis(10));
        beamReaderScheduler.start();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        beamReaderScheduler.stop();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Assert.assertEquals(5, beamReaderRuns.keySet().size());
        Assert.assertTrue(0 < beamReaderRuns.values()
                .stream()
                .flatMap(List::stream)
                .collect(Collectors.toList()).size()
                && 55 >= beamReaderRuns.values()
                .stream()
                .flatMap(List::stream)
                .collect(Collectors.toList()).size());
    }

    @Test
    public void testExceptionInConsumer() throws Exception {
        final AtomicInteger counter = new AtomicInteger(0);

        BeamConsumer beamConsumer = mockBeamConsumer((photonBeamReader, photonMessageHandler) -> {
            if (counter.intValue() < 100) {
                if (counter.intValue() % 3 == 0) {
                    counter.incrementAndGet();
                    throw new RuntimeException(String.format("This is an exception for %s", photonBeamReader.getBeamReaderUuid()));
                } else {
                    counter.incrementAndGet();
                    beamReaderRuns.computeIfAbsent(photonBeamReader, k -> Lists.newArrayList()).add(Instant.now());
                }
            }
        });

        PhotonScheduler beamReaderScheduler = new DefaultBeamReaderScheduler(beamReaderConfigManager, beamCache, beamReaderCache, beamConsumer,
                (photonBeamReaderLockWrapper, photonMessageHandler, cache, configManager, consumer, isAsync) ->
                        consumer.consume(photonBeamReaderLockWrapper.getPhotonBeamReader(), photonMessageHandler));

        for(int i = 0; i < 5; i++) {
            String client = String.format("ClientName_%s",i);
            String beam = String.format("BeamName_%s", i);

            PhotonBeam photonBeam = buildPhotonBeam();
            photonBeam.setBeamName(beam);
            beamCache.putBeam(photonBeam);

            beamReaderConfigManager.putBeamReaderConfig(new PhotonBeamReaderConfig(client, beam,
                    mock(PhotonMessageHandler.class), FROM_BEGINNING, (clientName, beamName) -> Instant.now()));

            beamReaderCache.getPhotonBeamReader(client, photonBeam).get().getPhotonBeamReader().setPhotonBeamReaderLock(new PhotonBeamReaderLock());
        }

        beamReaderScheduler.setPollingInterval(Duration.ofMillis(10));
        beamReaderScheduler.start();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        beamReaderScheduler.stop();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Assert.assertEquals(5, beamReaderRuns.keySet().size());
        Assert.assertTrue(60 < beamReaderRuns.values()
                .stream()
                .flatMap(List::stream)
                .collect(Collectors.toList()).size() && 70 >= beamReaderRuns.values()
                .stream()
                .flatMap(List::stream)
                .collect(Collectors.toList()).size());
    }

    @Test
    public void testExceptionWithBeamCache() throws Exception {

        BeamCache exceptionBeamCache = new MockBeamCacheWithException(mockBeamDao());

        BeamConsumer beamConsumer = mockBeamConsumer((photonBeamReader, photonMessageHandler) ->
                beamReaderRuns.computeIfAbsent(photonBeamReader, k -> Lists.newArrayList()).add(Instant.now()));

        PhotonScheduler beamReaderScheduler = new DefaultBeamReaderScheduler(beamReaderConfigManager, exceptionBeamCache,
                beamReaderCache, beamConsumer,
                (photonBeamReaderLockWrapper, photonMessageHandler, cache, configManager, consumer, isAsync) ->
                        consumer.consume(photonBeamReaderLockWrapper.getPhotonBeamReader(), photonMessageHandler));

        for(int i = 0; i < 5; i++) {
            String client = String.format("ClientName_%s",i);
            String beam = String.format("BeamName_%s", i);

            PhotonBeam photonBeam = buildPhotonBeam();
            photonBeam.setBeamName(beam);
            exceptionBeamCache.putBeam(photonBeam);

            PhotonBeamReader beamReader = buildPhotonBeamReader();
            beamReader.setClientName(client);
            beamReader.setBeamUuid(photonBeam.getBeamUuid());
            beamReader.setWaterMark(photonBeam.getStartDate());
            beamReader.setPhotonBeamReaderLock(new PhotonBeamReaderLock());

            beamReaderConfigManager.putBeamReaderConfig(new PhotonBeamReaderConfig(client, beam,
                    mock(PhotonMessageHandler.class), FROM_CURRENT, (clientName, beamName) -> Instant.now()));

            beamReaderCache.putPhotonBeamReader(photonBeam, beamReader);
        }

        beamReaderScheduler.setPollingInterval(Duration.ofMillis(10));
        beamReaderScheduler.start();
        try {
            Thread.sleep(120);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Assert.assertTrue(beamReaderScheduler.isActive());
        beamReaderScheduler.stop();
        Assert.assertFalse(beamReaderScheduler.isActive());
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Assert.assertEquals(5, beamReaderRuns.keySet().size());
        Assert.assertTrue(0 < beamReaderRuns.values()
                .stream()
                .flatMap(List::stream)
                .collect(Collectors.toList()).size()
                && 50 >= beamReaderRuns.values()
                .stream()
                .flatMap(List::stream)
                .collect(Collectors.toList()).size());
    }

    private static class MockBeamCacheWithException implements BeamCache {

        private final BeamDao beamDao;
        private final ConcurrentMap<String, Integer> beamCallCounter;

        public MockBeamCacheWithException(final BeamDao beamDao) {
            this(beamDao, Maps.newConcurrentMap());
        }

        public MockBeamCacheWithException(final BeamDao beamDao,
                                          final ConcurrentMap<String, Integer> beamCallCounter) {
            this.beamDao = beamDao;
            this.beamCallCounter = beamCallCounter;
        }

        @Override
        public Queue<PhotonBeam> getBeamByName(String beamName) {
            if (beamCallCounter.computeIfAbsent(beamName, k -> 0).equals(0)) {
                beamCallCounter.put(beamName, beamCallCounter.get(beamName) + 1);
                throw new RuntimeException(new TimeoutException());
            }
            return beamDao.getBeamByName(beamName);
        }

        @Override
        public Optional<PhotonBeam> getBeamByUuid(UUID beamUuid) {
            return beamDao.getBeamByUuid(beamUuid);
        }

        @Override
        public void putBeam(PhotonBeam photonBeam) {
            beamDao.putBeam(photonBeam);
        }

        @Override
        public Queue<PhotonBeam> getBeamByNameIfPresent(String beamName) {
            return beamDao.getBeamByName(beamName);
        }
    }
}
