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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class DefaultManifestTest {

    private SortedMap<Instant, Set<String>> manifestMap;
    private Manifest<Instant, String> manifest;

    @Before
    public void init() {
        manifestMap = new ConcurrentSkipListMap<>();
        manifest = new DefaultManifest<>(manifestMap);
    }

    @Test
    public void testPutEntry() {
        Instant now = Instant.now();
        Assert.assertTrue(manifestMap.isEmpty());
        manifest.putEntry(now, "TestEntry");
        Assert.assertFalse(manifestMap.isEmpty());
        Assert.assertEquals(1, manifestMap.size());
        manifest.putEntry(now, "TestEntry2");
        Assert.assertEquals(1, manifestMap.size());
    }

    @Test
    public void testRemoveEntry() {
        Instant now = Instant.now();
        Assert.assertTrue(manifestMap.isEmpty());
        manifest.putEntry(now, "TestEntry");
        manifest.putEntry(now, "TestEntry2");
        manifest.putEntry(now.plusMillis(10), "TestEntryThen");
        manifest.putEntry(now.plusMillis(10), "TestEntryThen2");
        manifest.putEntry(now.plusMillis(10), "TestEntryThen3");
        Assert.assertEquals(2, manifestMap.size());
        manifest.removeEntry(now, "TestEntry");
        Assert.assertEquals(2, manifestMap.size());
        manifest.removeEntry(now, "TestEntry2");
        Assert.assertEquals(1, manifestMap.size());
        manifest.removeEntry(now.plusMillis(10), "TestEntryThen");
        Assert.assertEquals(1, manifestMap.size());
        manifest.removeEntry(now.plusMillis(10), "TestEntryThen2");
        Assert.assertEquals(1, manifestMap.size());
        manifest.removeEntry(now.plusMillis(10), "TestEntryThen3");
        Assert.assertTrue(manifestMap.isEmpty());
    }

    @Test
    public void testGetEjectionKey() {
        Instant now = Instant.now();
        Optional<Instant> ejectionKey;
        Assert.assertTrue(manifestMap.isEmpty());
        manifest.putEntry(now, "TestEntry");
        manifest.putEntry(now, "TestEntry2");
        manifest.putEntry(now.plusMillis(5), "TestEntryThen");
        manifest.putEntry(now.plusMillis(10), "TestEntryThen2");
        manifest.putEntry(now.plusMillis(15), "TestEntryThen3");
        Assert.assertEquals(4, manifestMap.size());
        manifest.removeEntry(now, "TestEntry");
        ejectionKey = manifest.getEjectionKey();
        Assert.assertFalse(ejectionKey.isPresent());
        manifest.removeEntry(now, "TestEntry2");
        ejectionKey = manifest.getEjectionKey();
        Assert.assertTrue(ejectionKey.isPresent());
        Assert.assertEquals(ejectionKey.get(), now);
        manifest.removeEntry(now.plusMillis(10), "TestEntryThen2");
        ejectionKey = manifest.getEjectionKey();
        Assert.assertFalse(ejectionKey.isPresent());
        manifest.removeEntry(now.plusMillis(5), "TestEntryThen");
        ejectionKey = manifest.getEjectionKey();
        Assert.assertTrue(ejectionKey.isPresent());
        Assert.assertEquals(ejectionKey.get(), now.plusMillis(10));
    }
}