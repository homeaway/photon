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
package com.homeaway.datatools.photon.client.consumer;

import com.homeaway.datatools.photon.api.beam.AsyncPhotonConsumer;
import com.homeaway.datatools.photon.api.beam.BeamException;
import com.homeaway.datatools.photon.api.beam.MessageHandler;
import com.homeaway.datatools.photon.api.beam.PhotonConsumer;
import com.homeaway.datatools.photon.api.beam.PhotonMessage;
import com.homeaway.datatools.photon.api.beam.PhotonMessageHandler;
import com.homeaway.datatools.photon.api.model.enums.PhotonBeamReaderOffsetType;
import com.homeaway.datatools.photon.client.cache.ProcessedRecordCache;
import com.homeaway.datatools.photon.utils.processing.AsyncMessageProcessor;
import com.homeaway.datatools.photon.utils.processing.MessageEventHandler;
import com.homeaway.datatools.photon.utils.processing.MessageProcessorException;
import com.homeaway.datatools.photon.utils.processing.ProcessorManifest;
import com.homeaway.datatools.photon.utils.processing.Processors;
import static java.lang.Boolean.TRUE;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

public class DefaultAsyncPhotonConsumer<T> implements AsyncPhotonConsumer<T> {

    private final PhotonConsumer photonConsumer;
    private final AsyncMessageProcessor<PhotonProcessorKey, PhotonProcessorEvent<T>, Instant> asyncProcessor;

    public DefaultAsyncPhotonConsumer(final PhotonConsumer photonConsumer,
                                      final ProcessorManifest<PhotonProcessorKey, PhotonProcessorEvent<T>, Instant> manifest,
                                      final ProcessedRecordCache processedRecordCache,
                                      final int maxConcurrentEvents) {
        this(photonConsumer,
                Processors.newAsyncMessageProcessor(new PhotonMessageEventHandler<>(processedRecordCache), manifest),
                maxConcurrentEvents);
    }

    public DefaultAsyncPhotonConsumer(final PhotonConsumer photonConsumer,
                                      final AsyncMessageProcessor<PhotonProcessorKey, PhotonProcessorEvent<T>, Instant> asyncProcessor,
                                      final int maxConcurrentEvents) {
        this.photonConsumer = photonConsumer;
        this.asyncProcessor = asyncProcessor;
        this.asyncProcessor.setMaxEvents(maxConcurrentEvents);
    }

    @Override
    public void putBeamForProcessing(String clientName,
                                     String beamName,
                                     Function<PhotonMessage, T> eventMapper,
                                     MessageHandler<T, MessageProcessorException> eventAction,
                                     PhotonBeamReaderOffsetType offsetType) {

            photonConsumer.putBeamForAsyncProcessing(clientName, beamName,
                    new AsyncPhotonHandler<>(asyncProcessor, eventMapper, new PhotonProcessorKey(clientName, beamName), eventAction),
                    offsetType);
    }

    @Override
    public void putBeamForProcessing(String clientName,
                                     String beamName,
                                     Function<PhotonMessage, T> eventMapper,
                                     MessageHandler<T, MessageProcessorException> eventAction,
                                     PhotonBeamReaderOffsetType offsetType,
                                     BiFunction<String, String, Instant> waterMarkGetter) {
        photonConsumer.putBeamForAsyncProcessing(clientName, beamName,
                new AsyncPhotonHandler<>(asyncProcessor, eventMapper, new PhotonProcessorKey(clientName, beamName), eventAction),
                offsetType, waterMarkGetter);
    }

    @Override
    public void removeBeamFromProcessing(String clientName, String beamName) {
        photonConsumer.removeBeamFromProcessing(clientName, beamName);
    }

    @Override
    public void setPollingInterval(Long pollingInterval) {
        photonConsumer.setPollingInterval(pollingInterval);
    }

    @Override
    public Long getPollingInterval() {
        return photonConsumer.getPollingInterval();
    }

    @Override
    public void start() throws Exception {
        photonConsumer.start();
        asyncProcessor.start();
    }

    @Override
    public void stop() throws Exception {
        photonConsumer.stop();
        asyncProcessor.stop();
    }

    @Override
    public void shutdown() throws Exception {
        photonConsumer.shutdown();
        asyncProcessor.shutdown();
    }

    @Slf4j
    private static final class AsyncPhotonHandler<T> implements PhotonMessageHandler {

        private final AsyncMessageProcessor<PhotonProcessorKey, PhotonProcessorEvent<T>, Instant> asyncMessageProcessor;
        private final Function<PhotonMessage, T> eventMapper;
        private final PhotonProcessorKey photonProcessorKey;
        private final MessageHandler<T, MessageProcessorException> eventAction;

        public AsyncPhotonHandler(final AsyncMessageProcessor<PhotonProcessorKey, PhotonProcessorEvent<T>, Instant> asyncMessageProcessor,
                                  final Function<PhotonMessage, T> eventMapper,
                                  final PhotonProcessorKey photonProcessorKey,
                                  final MessageHandler<T, MessageProcessorException> eventAction) {
            this.asyncMessageProcessor = asyncMessageProcessor;
            this.eventMapper = eventMapper;
            this.photonProcessorKey = photonProcessorKey;
            this.eventAction = eventAction;
        }

        @Override
        public void handleMessage(PhotonMessage message) {
            asyncMessageProcessor.addEventToQueue(photonProcessorKey, getProcessorEvent(message, false));

        }

        @Override
        public void handleException(BeamException beamException) {
            throw new RuntimeException(beamException);
        }

        @Override
        public void handleStaleMessage(PhotonMessage message) {
            asyncMessageProcessor.addEventToQueue(photonProcessorKey, getProcessorEvent(message, true));
        }

        private PhotonProcessorEvent<T> getProcessorEvent(PhotonMessage message, boolean isStale) {
            T eventPayload = Optional.ofNullable(message.getPayloadBytes())
                    .map(p -> eventMapper.apply(message))
                    .orElse(null);
            return new PhotonProcessorEvent<>(message, eventPayload, message.getMessageKey(), message.getWriteTime(), eventAction, isStale);
        }
    }

    @Slf4j
    private static final class PhotonMessageEventHandler<T> implements MessageEventHandler<PhotonProcessorKey, PhotonProcessorEvent<T>> {

        private final ProcessedRecordCache processedRecordCache;

        public PhotonMessageEventHandler(final ProcessedRecordCache processedRecordCache) {
            this.processedRecordCache = processedRecordCache;
        }

        @Override
        public void handleEvent(PhotonProcessorKey key, PhotonProcessorEvent<T> event) {
            if (event.getPayload() != null) {
                if (event.isStale()) {
                    event.getEventAction().handleStaleMessage(event.getPayload());
                } else {
                    event.getEventAction().handleMessage(event.getPayload());
                }
                processedRecordCache.putEntry(event.getPhotonMessage(), TRUE);
            }
        }

        @Override
        public void handleException(PhotonProcessorKey key, PhotonProcessorEvent<T> event, MessageProcessorException exception) {
            event.getEventAction().handleException(exception);
        }
    }
}
