package com.homeaway.datatools.photon.serialization;

import com.homeaway.datatools.photon.client.PhotonClientTestHelper;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import lombok.Data;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;

public class JsonSerializationTest {

    private PhotonSerializer photonSerializer;
    private PhotonDeserializer photonDeserializer;

    @Before
    public void init() {
        photonSerializer = new JsonPhotonSerializer();
        photonDeserializer = new JsonPhotonDeserializer();
    }

    @Test
    public void testSerializationDeserialization() {
        PhotonBeam photonBeam = PhotonClientTestHelper.buildPhotonBeam();
        TestClass testClass = new TestClass();
        testClass.setTestDate(Instant.now().toEpochMilli());
        testClass.setTestInt(10);
        testClass.setTestString("Testing String");
        byte[] bytes = photonSerializer.serialize(photonBeam, testClass);
        Assert.assertNotNull(bytes);
        Assert.assertTrue(bytes.length > 0);
        TestClass newClass = photonDeserializer.deserialize(photonBeam, bytes, TestClass.class);
        Assert.assertEquals(testClass, newClass);
    }

    @Data
    private static class TestClass {
        private String testString;
        private int testInt;
        private long testDate;
    }
}
