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

import java.time.Instant;
import java.util.UUID;

/**
 * The message that is produced by a photon consumer and passed off to Handler.
 */
public interface PhotonMessage {

    /**
     * The UUID of the beam reader that read this message.
     *
     * @return - UUID of the beam reader.
     */
    UUID getBeamReaderUuid();

    /**
     * The time slice partition in which this data lives.
     *
     * @return - Instant that represents the time slice partition the event was written
     */
    Instant getPartitionTime();

    /**
     * The time that the message was persisted to Photon
     *
     * @return - Instant that represents the time the event was written
     */
    Instant getWriteTime();

    /**
     * The key of the message.
     *
     * @return - String that is the key of the message.
     */
    String getMessageKey();

    /**
     * The payload object of the message.
     *
     * @param clazz - The class to deserialize the payload into.
     * @param <T> - The type of the object that is persisted in the payload (inferred from the client).
     * @return - Object of type T that is used as the payload.
     */
    <T> T getPayload(Class<T> clazz);

    /**
     * The payload of the message in JSON.
     *
     * @return A JSON string of the message payload.
     */
    String getPayloadString();

    /**
     * The raw bytes of the message payload.
     * @return A big endian byte array of the raw bytes of the message payload.
     */
    byte[] getPayloadBytes();
}
