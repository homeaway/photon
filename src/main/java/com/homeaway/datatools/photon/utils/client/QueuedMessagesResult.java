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

import com.homeaway.datatools.photon.dao.rows.PhotonRowSetFuture;
import lombok.Data;
import lombok.Getter;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Data
public class QueuedMessagesResult {

    @Getter
    private final Instant partitionTime;

    private final List<PhotonRowSetFuture> queuedMessages;

    @Getter
    private final ProcessedRecordCacheCheck processedRecordCacheCheck;

    public Optional<List<PhotonRowSetFuture>> getQueueMessages() {
        return Optional.ofNullable(queuedMessages);
    }

}
