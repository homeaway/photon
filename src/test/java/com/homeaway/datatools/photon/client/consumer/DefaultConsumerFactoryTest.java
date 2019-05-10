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
package com.homeaway.datatools.photon.client.consumer;

import com.homeaway.datatools.photon.api.beam.AsyncPhotonConsumer;
import com.homeaway.datatools.photon.api.beam.ConsumerFactory;
import com.homeaway.datatools.photon.api.beam.PhotonConsumer;
import com.homeaway.datatools.photon.client.cache.BeamCache;
import com.homeaway.datatools.photon.client.cache.BeamReaderCache;
import com.homeaway.datatools.photon.client.cache.ProcessedRecordCache;
import com.homeaway.datatools.photon.dao.beam.BeamReaderDao;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.mock;

import java.util.UUID;

public class DefaultConsumerFactoryTest {

    private ConsumerFactory consumerFactory;

    @Before
    public void init() {
        consumerFactory = new DefaultConsumerFactory(mock(PhotonConsumer.class), mock(BeamCache.class),
                mock(BeamReaderCache.class), mock(BeamReaderDao.class), mock(ProcessedRecordCache.class));
    }

    @Test
    public void testGetConsumer() {
        PhotonConsumer consumer = consumerFactory.getPhotonConsumer();
        Assert.assertNotNull(consumer);
    }

    @Test
    public void testGetAsyncConsumer() {
        AsyncPhotonConsumer<UUID> asyncPhotonConsumer = consumerFactory.getAsyncPhotonConsumer();
        Assert.assertNotNull(asyncPhotonConsumer);
    }
}
