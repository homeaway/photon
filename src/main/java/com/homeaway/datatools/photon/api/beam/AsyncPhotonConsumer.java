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
package com.homeaway.datatools.photon.api.beam;

import com.homeaway.datatools.photon.api.model.enums.PhotonBeamReaderOffsetType;
import com.homeaway.datatools.photon.utils.processing.MessageProcessorException;

import java.time.Instant;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A Photon Consumer that is integrated with an asynchronous processing queue.
 * @param <T> - The type of object to be passed off the asynchronous processing queue.
 */
public interface AsyncPhotonConsumer<T> extends BasePhotonConsumer {

    /**
     * A method to add a consumer of a beam consumption and processing through an asynchronous processing queue.
     * @param clientName - The client name of the consumer.
     * @param beamName - The name of the beam to be consumed.
     * @param eventMapper - A Function to map a PhotonMessage to an object of type T to be passed off to the processing queue.
     * @param eventAction - The Consumer that will be accept the object from the asynchronous processing queue. NOTE: this will be
     *                    executed on multiple threads and thus should be implemented in a threadsafe manner.
     * @param offsetType - The type of offset to use for the reader.
     */
    void putBeamForProcessing(String clientName,
                              String beamName,
                              Function<PhotonMessage, T> eventMapper,
                              MessageHandler<T, MessageProcessorException> eventAction,
                              PhotonBeamReaderOffsetType offsetType);

    /**
     * A method to add a consumer of a beam consumption and processing through an asynchronous processing queue.
     * @param clientName - The client name of the consumer.
     * @param beamName - The name of the beam to be consumed.
     * @param eventMapper - A Function to map a PhotonMessage to an object of type T to be passed off to the processing queue.
     * @param eventAction - The Consumer that will be accept the object from the asynchronous processing queue. NOTE: this will be
     *                    executed on multiple threads and thus should be implemented in a threadsafe manner.
     * @param offsetType - The type of offset to use for the reader.
     * @param waterMarkGetter - A function that takes a PhotonBeamReader and then returns the Instant that should be used for the initial watermark value.
     */
    void putBeamForProcessing(String clientName,
                              String beamName,
                              Function<PhotonMessage, T> eventMapper,
                              MessageHandler<T, MessageProcessorException> eventAction,
                              PhotonBeamReaderOffsetType offsetType,
                              BiFunction<String, String, Instant> waterMarkGetter);
}
