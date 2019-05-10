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

import com.homeaway.datatools.photon.api.beam.BeamException;
import com.homeaway.datatools.photon.api.beam.PhotonMessage;
import com.homeaway.datatools.photon.api.beam.PhotonMessageHandler;
import com.homeaway.datatools.photon.api.model.enums.PhotonBeamReaderOffsetType;
import com.homeaway.datatools.photon.client.consumer.PhotonBeamReaderConfig;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;

@Slf4j
public class PhotonBeamReaderConfigTest {

    @Test
    public void testConstructors() {
        PhotonMessageHandler handler = new PhotonMessageHandler() {
            @Override
            public void handleMessage(PhotonMessage message) {
                log.info("Handle message");
            }

            @Override
            public void handleException(BeamException beamException) {
                log.info("Handle exception");
            }

            @Override
            public void handleStaleMessage(PhotonMessage message) {

            }
        };
        Instant waterMark = Instant.now().minusSeconds(30);
        PhotonBeamReaderConfig config = new PhotonBeamReaderConfig("TestClientName", "TestBeamName", handler, PhotonBeamReaderOffsetType.FROM_BEGINNING,
                (clientName, beamName) -> waterMark);
        Assert.assertNotNull(config.getWaterMarkGetter());
        Assert.assertEquals(PhotonBeamReaderOffsetType.FROM_BEGINNING, config.getOffsetType());
        Assert.assertEquals(waterMark, config.getWaterMarkGetter().apply("TestClientName", "TestBeamName"));
    }
}
