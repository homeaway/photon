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
package com.homeaway.datatools.photon.utils.processing;

import lombok.Data;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;

public class ProcessorsTest {

    @Test
    public void testAsyncProcessorNoManifest() {
        AsyncMessageProcessor<TestProcessorKey, TestEvent, Instant> processor =
                Processors.newAsyncMessageProcessor(new MessageEventHandler<TestProcessorKey, TestEvent>() {
                    @Override
                    public void handleEvent(TestProcessorKey key, TestEvent event) {

                    }

                    @Override
                    public void handleException(TestProcessorKey key, TestEvent event, MessageProcessorException exception) {

                    }
                });
        Assert.assertFalse(processor.getProcessorManifest().isPresent());
    }

    @Test
    public void testAsyncProcessorWithManifest() {
        AsyncMessageProcessor<TestProcessorKey, TestEvent, Instant> processor =
                Processors.newAsyncMessageProcessor(new MessageEventHandler<TestProcessorKey, TestEvent>() {
                    @Override
                    public void handleEvent(TestProcessorKey key, TestEvent event) {

                    }

                    @Override
                    public void handleException(TestProcessorKey key, TestEvent event, MessageProcessorException exception) {

                    }
                }, new DefaultProcessorManifest<>((key, date) -> {
                    TestProcessorKey testKey = key;
                }));
        Assert.assertTrue(processor.getProcessorManifest().isPresent());
    }

    @Data
    private static class TestEvent implements ProcessingEvent<Instant> {

        private final Instant eventDate;
        private final String eventKey;

        public TestEvent(final Instant eventDate,
                         final String eventKey) {
            this.eventDate = eventDate;
            this.eventKey = eventKey;
        }

        @Override
        public String getEventKey() {
            return eventKey;
        }

        @Override
        public Instant getEventOrderingKey() {
            return eventDate;
        }
    }

    @Data
    private static class TestProcessorKey implements ProcessorKey {

        private String beamName;

        @Override
        public String getKeyValue() {
            return beamName;
        }
    }

}
