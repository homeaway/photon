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


import java.time.Duration;
import java.time.Instant;

/**
 * The BeamProducer is used to write messages to the photon beam.
 */
public interface BeamProducer {

    /**
     * This method is used to write messages to photon without specifying a write time and using the default
     * TTL value of the beam. If the beam has not yet been created then it will be created without a default
     * TTL allowing messages to live forever.
     *
     * @param beamName - The name of the beam you are writing to
     * @param messageKey - The key of message you are writing
     * @param payload - The payload of the message
     */
    void writeMessageToBeam(String beamName, String messageKey, Object payload);

    /**
     * This method is used to write a message to photon without specifying a write time and using a specific TTL
     * value. If the beam does not already exist then it will be created with the specified TTL as the default,
     * if the beam already exists then the message will be written with the specified TTL value instead of the default.
     *
     * @param beamName - The name of the beam you are writing to
     * @param messageKey - The key of message you are writing
     * @param payload - The payload of the message
     * @param ttl - The Time to Live of the message
     */
    void writeMessageToBeam(String beamName, String messageKey, Object payload, Duration ttl);

    /**
     * This method is used to write a message to photon specifying a write time and not specifying a TTL
     * value. If the beam does not already exist then it will be created with no TTL as the default,
     * if the beam already exists then it will use the default TTL value for the beam.
     *
     * Note: This should only be used for creating events that are planned for the future, if the provided
     * write time is less than or equal to 5 seconds in the future then the provided value will not be used.
     *
     * @param beamName - The name of the beam you are writing to
     * @param messageKey - The key of message you are writing
     * @param payload - The payload of the message
     * @param writeTime - The write time that will used to sequence the message
     */
    void writeMessageToBeam(String beamName, String messageKey, Object payload, Instant writeTime);

    /**
     * This method is used to write a message to photon specifying a write time and a TTL value.
     * If the beam does not already exist then it will be created with with the specified TTL as the default,
     * if the beam already exists then the message will written with the specified TTL instead of the default for the beam.
     *
     * @param beamName - The name of the beam you are writing to
     * @param messageKey - The key of message you are writing
     * @param payload - The payload of the message
     * @param writeTime - The write time that will used to sequence the message
     * @param ttl - The Time to Live of the message
     */
    void writeMessageToBeam(String beamName, String messageKey, Object payload, Instant writeTime, Duration ttl);

    /**
     * This method may be used to delete a message from photon (this should be exceedingly rare and only used in the case of a future
     * planned event that must be cancelled).
     *
     * @param beamName - The name of the beam you are deleting from
     * @param messageKey - The key of message you are deleting
     * @param writeTime - The exact time the message was written to the millisecond (this will only be available for future planned events)
     */
    void deleteMessageFromBeam(String beamName, String messageKey, Instant writeTime);

    /**
     * This method is used to write messages to photon asynchronously without specifying a write time and using the default
     * TTL value of the beam. If the beam has not yet been created then it will be created without a default
     * TTL allowing messages to live forever.
     *
     * @param beamName - The name of the beam you are writing to
     * @param messageKey - The key of message you are writing
     * @param payload - The payload of the message
     * @return BeamFuture that can be used to wait on action to complete.
     */
    BeamFuture writeMessageToBeamAsync(String beamName, String messageKey, Object payload);

    /**
     * This method is used to write a message to photon asynchronously without specifying a write time and using a specific TTL
     * value. If the beam does not already exist then it will be created with the specified TTL as the default,
     * if the beam already exists then the message will be written with the specified TTL value instead of the default.
     *
     * @param beamName - The name of the beam you are writing to
     * @param messageKey - The key of message you are writing
     * @param payload - The payload of the message
     * @param ttl - The Time to Live of the message
     * @return BeamFuture that can be used to wait on action to complete.
     */
    BeamFuture writeMessageToBeamAsync(String beamName, String messageKey, Object payload, Duration ttl);

    /**
     * This method is used to write a message to photon asynchronously specifying a write time and not specifying a TTL
     * value. If the beam does not already exist then it will be created with no TTL as the default,
     * if the beam already exists then it will use the default TTL value for the beam.
     *
     * Note: This should only be used for creating events that are planned for the future, if the provided
     * write time is less than or equal to 5 seconds in the future then the provided value will not be used.
     *
     * @param beamName - The name of the beam you are writing to
     * @param messageKey - The key of message you are writing
     * @param payload - The payload of the message
     * @param writeTime - The write time that will used to sequence the message
     * @return BeamFuture that can be used to wait on action to complete.
     */
    BeamFuture writeMessageToBeamAsync(String beamName, String messageKey, Object payload, Instant writeTime);

    /**
     * This method is used to write a message to photon asynchronously specifying a write time and a TTL value.
     * If the beam does not already exist then it will be created with with the specified TTL as the default,
     * if the beam already exists then the message will written with the specified TTL instead of the default for the beam.
     *
     * @param beamName - The name of the beam you are writing to
     * @param messageKey - The key of message you are writing
     * @param payload - The payload of the message
     * @param writeTime - The write time that will used to sequence the message
     * @param ttl - The Time to Live of the message
     * @return BeamFuture that can be used to wait on action to complete.
     */
    BeamFuture writeMessageToBeamAsync(String beamName, String messageKey, Object payload, Instant writeTime, Duration ttl);

    /**
     * This method may be used to delete a message asynchronously from photon (this should be exceedingly rare and only used in the case of a future
     * planned event that must be cancelled).
     *
     * @param beamName - The name of the beam you are deleting from
     * @param messageKey - The key of message you are deleting
     * @param writeTime - The exact time the message was written to the millisecond (this will only be available for future planned events)
     * @return BeamFuture that can be used to wait on action to complete.
     */
    BeamFuture deleteMessageFromBeamAsync(String beamName, String messageKey, Instant writeTime);

    /**
     * This method will set the size of the partition time slices in milliseconds.
     *
     * @return The partition time slice in milliseconds.
     */
    int getPartitionSize();

    /**
     * This method will set the size of the partition time slices in milliseconds.
     *
     * @param partitionSize - The new size of the partition time slice in milliseconds.
     */
    void setPartitionSize(int partitionSize);
}
