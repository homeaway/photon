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
package com.homeaway.datatools.photon.utils.client;

import com.google.common.collect.Lists;
import com.homeaway.datatools.photon.api.beam.PhotonMessageHandler;
import com.homeaway.datatools.photon.api.model.PhotonBeamReaderLockWrapper;
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
import static com.homeaway.datatools.photon.utils.client.ConsumerUtils.CONSUMER_EXECUTION_FUNCTION;
import static com.homeaway.datatools.photon.utils.client.ConsumerUtils.GET_BEAM_WITH_EARLIEST_START;
import static com.homeaway.datatools.photon.utils.client.ConsumerUtils.GET_PHOTON_ROWSET_FROM_FUTURE;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;
import static com.homeaway.datatools.photon.api.model.enums.PhotonBeamReaderOffsetType.FROM_CURRENT;
import com.homeaway.datatools.photon.dao.rows.PhotonRowSet;
import com.homeaway.datatools.photon.dao.rows.PhotonRowSetFuture;
import static com.homeaway.datatools.photon.utils.client.ConsumerUtils.GET_WATERMARK;
import com.homeaway.datatools.photon.utils.client.consumer.BeamReaderConfigManager;
import com.homeaway.datatools.photon.utils.client.consumer.DefaultBeamReaderConfigManager;
import static java.lang.Boolean.FALSE;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import static org.mockito.Mockito.mock;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public class ConsumerUtilsTest {

    @Test
    public void getResultSetFromFutureTest() {
        PhotonRowSet photonRowSet = GET_PHOTON_ROWSET_FROM_FUTURE.apply(new MockPhotonRowSetFuture(0));
        Assert.assertNotNull(photonRowSet);
    }

    @Test (expected = RuntimeException.class)
    public void getResultSetFromFutureTestFailure() {
        PhotonRowSetFuture future = new MockPhotonRowSetFuture(10);
        PhotonRowSet photonRowSet = GET_PHOTON_ROWSET_FROM_FUTURE.apply(future);
    }

    @Test
    public void getResultSetFromFutureTestFailureCacnelled() {
        PhotonRowSetFuture future = new MockPhotonRowSetFuture(10);
        try {
            PhotonRowSet photonRowSet = GET_PHOTON_ROWSET_FROM_FUTURE.apply(future);
        } catch (Exception e) {
            log.error("Error getting future", e);
        }
        Assert.assertTrue(future.isCancelled());
    }

    @Test
    public void testGetBeamWithEarliestStart() {
        Queue<PhotonBeam> beams = Lists.newLinkedList();

        PhotonBeam beam = buildPhotonBeam();
        beam.setStartDate(Instant.now().minusSeconds(5));
        beams.add(beam);

        beam = buildPhotonBeam();
        beam.setStartDate(Instant.now().minusSeconds(1));
        beams.add(beam);

        beam = buildPhotonBeam();
        beam.setStartDate(Instant.now().minusSeconds(10));
        beams.add(beam);

        PhotonBeam earliestBeam = buildPhotonBeam();
        earliestBeam.setStartDate(Instant.now().minusSeconds(20));
        beams.add(earliestBeam);

        beam = buildPhotonBeam();
        beam.setStartDate(Instant.now().minusSeconds(7));
        beams.add(beam);

        PhotonBeam resultBeam = GET_BEAM_WITH_EARLIEST_START.apply(beams);
        Assert.assertEquals(earliestBeam, resultBeam);

        beams.forEach(b -> {
            if (!b.equals(resultBeam)) {
                Assert.assertTrue(b.getStartDate().isAfter(resultBeam.getStartDate()));
            }
        });
    }

    @Test
    public void testGetBeamWithEarliestStartEmptyQueue() {
        Queue<PhotonBeam> beams = Lists.newLinkedList();
        PhotonBeam beam = GET_BEAM_WITH_EARLIEST_START.apply(beams);
        Assert.assertNull(beam);
    }

    @Test
    public void testCheckWaterMark() {

        Instant waterMark = Instant.now().minusSeconds(10);

        PhotonBeam beam = buildPhotonBeam();

        PhotonBeamReader beamReader = buildPhotonBeamReader();
        beamReader.setWaterMark(null);
        beamReader.setBeamUuid(beam.getBeamUuid());

        PhotonBeamReaderConfig beamReaderConfig = new PhotonBeamReaderConfig(beamReader.getClientName(), beam.getBeamName(),
                mock(PhotonMessageHandler.class), FROM_CURRENT, (clientName, beamName) -> waterMark);

        PhotonBeamReaderLockWrapper wrapper = PhotonBeamReaderLockWrapper.of(beamReader);

        BeamCache beamCache = new DefaultBeamCache(mockBeamDao());
        BeamReaderCache beamReaderCache = new DefaultBeamReaderCache(mockBeamReaderDao());

        beamCache.putBeam(beam);

        BeamReaderConfigManager configManager = new DefaultBeamReaderConfigManager(beamCache, beamReaderCache);
        configManager.putBeamReaderConfig(beamReaderConfig);

        Optional<Instant> waterMarkOptional = GET_WATERMARK.get(wrapper, beamCache, configManager);

        Assert.assertTrue(waterMarkOptional.isPresent());
        Assert.assertEquals(waterMark, waterMarkOptional.get());
    }

    @Test
    public void testCheckWaterMarkNoBeam() {

        Instant waterMark = Instant.now().minusSeconds(10);

        PhotonBeamReader beamReader = buildPhotonBeamReader();
        beamReader.setWaterMark(null);
        beamReader.setBeamUuid(UUID.randomUUID());

        PhotonBeamReaderConfig beamReaderConfig = new PhotonBeamReaderConfig(beamReader.getClientName(), "TestNoBeam",
                mock(PhotonMessageHandler.class), FROM_CURRENT, (clientName, beamName) -> waterMark);

        PhotonBeamReaderLockWrapper wrapper = PhotonBeamReaderLockWrapper.of(beamReader);

        BeamCache beamCache = new DefaultBeamCache(mockBeamDao());
        BeamReaderCache beamReaderCache = new DefaultBeamReaderCache(mockBeamReaderDao());

        BeamReaderConfigManager configManager = new DefaultBeamReaderConfigManager(beamCache, beamReaderCache);
        configManager.putBeamReaderConfig(beamReaderConfig);

        Optional<Instant> waterMarkOptional = GET_WATERMARK.get(wrapper, beamCache, configManager);

        Assert.assertFalse(waterMarkOptional.isPresent());
    }

    @Test
    public void testConsumerExecutionFunction() {

        List<PhotonBeamReader> results = Lists.newArrayList();

        PhotonBeam beam = buildPhotonBeam();

        PhotonBeamReader beamReader = buildPhotonBeamReader();
        beamReader.setBeamUuid(beam.getBeamUuid());

        PhotonBeamReaderConfig beamReaderConfig = new PhotonBeamReaderConfig(beamReader.getClientName(), beam.getBeamName(),
                mock(PhotonMessageHandler.class), FROM_CURRENT, (clientName, beamName) -> Instant.now());

        PhotonBeamReaderLockWrapper wrapper = PhotonBeamReaderLockWrapper.of(beamReader);

        BeamCache beamCache = new DefaultBeamCache(mockBeamDao());
        BeamReaderCache beamReaderCache = new DefaultBeamReaderCache(mockBeamReaderDao());

        beamCache.putBeam(beam);

        BeamReaderConfigManager configManager = new DefaultBeamReaderConfigManager(beamCache, beamReaderCache);
        configManager.putBeamReaderConfig(beamReaderConfig);

        BeamConsumer beamConsumer = mockBeamConsumer((photonBeamReader, photonMessageHandler) ->
                results.add(photonBeamReader));

        CONSUMER_EXECUTION_FUNCTION.execute(wrapper, mock(PhotonMessageHandler.class), beamCache,
                configManager, beamConsumer, FALSE);

        Assert.assertEquals(1, results.size());
        Assert.assertFalse(wrapper.getLock().isLocked());

    }

    @Test
    public void testConsumerExecutionFunctionLocked() {

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        List<PhotonBeamReader> results = Lists.newArrayList();

        PhotonBeam beam = buildPhotonBeam();

        PhotonBeamReader beamReader = buildPhotonBeamReader();
        beamReader.setBeamUuid(beam.getBeamUuid());

        PhotonBeamReaderConfig beamReaderConfig = new PhotonBeamReaderConfig(beamReader.getClientName(), beam.getBeamName(),
                mock(PhotonMessageHandler.class), FROM_CURRENT, (clientName, beamName) -> Instant.now());

        PhotonBeamReaderLockWrapper wrapper = PhotonBeamReaderLockWrapper.of(beamReader);
        wrapper.getLock().tryLock();

        BeamCache beamCache = new DefaultBeamCache(mockBeamDao());
        BeamReaderCache beamReaderCache = new DefaultBeamReaderCache(mockBeamReaderDao());

        beamCache.putBeam(beam);

        BeamReaderConfigManager configManager = new DefaultBeamReaderConfigManager(beamCache, beamReaderCache);
        configManager.putBeamReaderConfig(beamReaderConfig);


        BeamConsumer beamConsumer = mockBeamConsumer((photonBeamReader, photonMessageHandler) ->
                results.add(photonBeamReader));

        Future<Boolean> future = executorService.submit(() -> {
            CONSUMER_EXECUTION_FUNCTION.execute(wrapper, mock(PhotonMessageHandler.class), beamCache,
                    configManager, beamConsumer, FALSE);
            return true;
        });

        Assert.assertEquals(0, results.size());
        Assert.assertTrue(wrapper.getLock().isLocked());

    }

    @Test
    public void testConsumerExecutionFunctionConsumerException() {

        List<PhotonBeamReader> results = Lists.newArrayList();

        PhotonBeam beam = buildPhotonBeam();

        PhotonBeamReader beamReader = buildPhotonBeamReader();
        beamReader.setBeamUuid(beam.getBeamUuid());

        PhotonBeamReaderConfig beamReaderConfig = new PhotonBeamReaderConfig(beamReader.getClientName(), beam.getBeamName(),
                mock(PhotonMessageHandler.class), FROM_CURRENT, (clientName, beamName) -> Instant.now());

        PhotonBeamReaderLockWrapper wrapper = PhotonBeamReaderLockWrapper.of(beamReader);

        BeamCache beamCache = new DefaultBeamCache(mockBeamDao());
        BeamReaderCache beamReaderCache = new DefaultBeamReaderCache(mockBeamReaderDao());

        beamCache.putBeam(beam);

        BeamReaderConfigManager configManager = new DefaultBeamReaderConfigManager(beamCache, beamReaderCache);
        configManager.putBeamReaderConfig(beamReaderConfig);

        BeamConsumer beamConsumer = mockBeamConsumer((photonBeamReader, photonMessageHandler) -> {
            throw new RuntimeException("This is an exception");
        });

        CONSUMER_EXECUTION_FUNCTION.execute(wrapper, mock(PhotonMessageHandler.class), beamCache,
                configManager, beamConsumer, FALSE);

        Assert.assertEquals(0, results.size());
        Assert.assertFalse(wrapper.getLock().isLocked());

    }

    private static class MockPhotonRowSetFuture implements PhotonRowSetFuture {

        private final long waitTime;
        private boolean cancelled;

        public MockPhotonRowSetFuture (long waitTime) {
            this.waitTime = waitTime;
            cancelled = false;
        }

        @Override
        public PhotonRowSet getUninterruptibly() {
            return mock(PhotonRowSet.class);
        }

        @Override
        public PhotonRowSet getUninterruptibly(long timeout, TimeUnit unit) throws TimeoutException {
            if (timeout < waitTime) {
                throw  new TimeoutException();
            } else {
                return mock(PhotonRowSet.class);
            }
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            cancelled = true;
            return true;
        }

        @Override
        public void addListener(Runnable listener, Executor executor) {

        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        public PhotonRowSet get() throws InterruptedException, ExecutionException {
            return null;
        }

        @Override
        public PhotonRowSet get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return null;
        }
    }

}
