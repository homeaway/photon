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

public interface BasePhotonConsumer extends Startable {

    /**
     * Method to remove a particular beam for a specific reader from the scheduler.
     *
     * @param clientName - The name assigned to the reader by the end-user app.
     * @param beamName - The name of the beam to stop processing.
     */
    void removeBeamFromProcessing(String clientName, String beamName);

    /**
     * Method to configure the polling interval for the scheduler.
     *
     * @param pollingInterval - The interval to be used in milliseconds.
     */
    void setPollingInterval(Long pollingInterval);

    /**
     * Method to fetch the polling interval for the scheduler.
     *
     * @return - The polling interval in milliseconds for the scheduler.
     */
    Long getPollingInterval();


}
