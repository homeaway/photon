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

import java.time.Duration;
import java.time.temporal.ChronoUnit;

public final class ClientConstants {

    public static final int PARTITION_SIZE_MILLISECONDS = 100;
    public static final int WATERMARK_MANAGER_WINDOW = 15;
    public static final ChronoUnit WATERMAKR_MANAGER_WINDOW_UNIT = ChronoUnit.SECONDS;
    public static final long INTERVAL_SECONDS = 10;
    public static final Duration BEAM_READ_LOCK_THRESHOLD = Duration.ofSeconds(10);
    public static final Duration DEFAULT_WALKBACK_THRESHOLD = Duration.ofDays(7);
}
