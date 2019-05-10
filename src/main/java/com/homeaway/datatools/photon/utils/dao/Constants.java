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
package com.homeaway.datatools.photon.utils.dao;

import java.time.Duration;

public final class Constants {

    public static final String BEAM_SCHEMA_TABLE = "beam_schema";
    public static final String SCHEMA_COLUMN = "schema_text";
    public static final String BEAM_BY_NAME_TABLE = "beam_by_name";
    public static final String BEAM_BY_UUID_TABLE = "beam_by_uuid";
    public static final String BEAM_READER_TABLE = "beam_reader";
    public static final String BEAM_QUEUE_TABLE = "beam_data";
    public static final String BEAM_PROCESSED_QUEUE_TABLE = "beam_processed_data";
    public static final String BEAM_DATA_MASTER_MANIFEST_TABLE = "beam_data_master_manifest";
    public static final String BEAM_DATA_MANIFEST_TABLE = "beam_data_manifest";
    public static final String BEAM_NAME_COLUMN = "beam_name";
    public static final String BEAM_UUID_COLUMN = "beam_uuid";
    public static final String BEAM_DEFAULT_TTL_COLUMN = "default_ttl";
    public static final String START_DATE_COLUMN = "start_date";
    public static final String CLIENT_NAME_COLUMN =  "client_name";
    public static final String BEAM_READER_UUID_COLUMN = "beam_reader_uuid";
    public static final String WATERMARK_COLUMN = "watermark";
    public static final String BEAM_READER_LOCK_UUID_COLUMN = "lock_uuid";
    public static final String BEAM_READER_LOCK_TIME_COLUMN = "lock_time";
    public static final String MESSAGE_KEY_COLUMN = "message_key";
    public static final String WRITE_TIME_COLUMN = "write_time";
    public static final String PARTITION_TIME_COLUMN = "partition_time";
    public static final String PAYLOAD_COLUMN = "payload_blob";
    public static final String CLUSTER_NOW_COLUMN = "now";
    public static final String MANIFEST_TIME_COLUMN = "manifest_time";
    public static final String MASTER_MANIFEST_TIME_COLUMN = "master_manifest_time";
    public static final Duration MANIFEST_PARTITION_SIZE = Duration.ofMillis(5000);
    public static final Duration MASTER_MANIFEST_PARTITION_SIZE = Duration.ofHours(6);
}
