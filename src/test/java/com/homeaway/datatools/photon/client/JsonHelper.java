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
package com.homeaway.datatools.photon.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JsonHelper {

    private static ObjectMapper mapper = new ObjectMapper()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .registerModule(new Jdk8Module())
            .registerModule(new JavaTimeModule());


    public static <T> T jsonRoundTrip(T object, Class<T> clazz) {
        return getObjectFromJson(getJson(object), clazz);
    }

    public static <T> T jsonRoundTrip(T object, TypeReference type) {
        return getObjectFromJson(getJson(object), type);
    }

    public static String getJson(Object object) {
        try {
            return mapper.writeValueAsString(object);
        } catch (Exception e) {
            log.error("Could not serialize object {}", object, e);
        }
        return null;
    }

    public static <T> T getObjectFromJson(String json, Class<T> clazz) {
        try {
            return mapper.readValue(json, clazz);
        } catch (Exception e) {
            log.error("Could not deserialize json {} into {}", json, clazz.getName(), e);
        }
        return null;
    }

    public static <T> T getObjectFromJson(String json, TypeReference type) {
        try {
            return mapper.readValue(json, type);
        } catch (Exception e) {
            log.error("Could not deserialize json {} into {}", json, type.getType(), e);
        }
        return null;
    }

}