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

/**
 * This class is basically a factory for building a Photon Consumer or an AsyncPhotonConsumer.
 */
public interface ConsumerFactory {

    /**
     * A method for building a synchronous Photon Consumer.
     * @return A PhotonConsumer
     */
    PhotonConsumer getPhotonConsumer();

    /**
     * A method to build an photon consumer that is integrated with an asynchronous event processor that will preservice
     * order by beam reader and message key. (i.e. if a beam reader encounters multiple events with the same key then
     * those events will be processed in the order in which they arrived but each key will be processed on a separate thread).
     * This implementation will not limit the number of events that can be in the asynchronous processor at a given time.
     * @param <T> - The type of event that will be handed to the event asynchronous processor.
     * @return A AsyncPhotonConsumer
     */
    <T> AsyncPhotonConsumer<T> getAsyncPhotonConsumer();

    /**
     * A method to build an photon consumer that is integrated with an asynchronous event processor that will preservice
     * order by beam reader and message key. (i.e. if a beam reader encounters multiple events with the same key then
     * those events will be processed in the order in which they arrived but each key will be processed on a separate thread).
     * This implementation limits the number of events that can be in the asynchronous processor at a given time, if that limit is reached
     * then the processor will not accept new events until space has been made by processing older events.
     * @param <T> - The type of event that will be handed to the event asynchronous processor.
     * @param maxConcurrentEvents - The maximum number of events that can be held in the asynchronous processor.
     * @return A AsyncPhotonConsumer
     */
    <T> AsyncPhotonConsumer<T> getAsyncPhotonConsumer(int maxConcurrentEvents);

    /**
     * Method to configure the polling interval for the scheduler.
     *
     * @param pollingInterval - The interval to be used in milliseconds.
     */
    void setPollingInterval(Long pollingInterval);
}
