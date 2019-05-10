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
package com.homeaway.datatools.photon.client.consumer.partition.consumer.watermark;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class BaseWatermarkManagerTest {

    private static final int WINDOW = 5;
    private static final ChronoUnit WINDOW_UNIT = ChronoUnit.SECONDS;
    private WatermarkManager watermarkManager;

    @Before
    public void init() {
        watermarkManager = new BaseWatermarkManager(WINDOW, WINDOW_UNIT);
    }

    @Test
    public void calculateNewWatermarkTest() {
        Assert.assertEquals(5, watermarkManager.calculateNewWatermark(Instant.now()).until(Instant.now(), ChronoUnit.SECONDS));
    }
}
