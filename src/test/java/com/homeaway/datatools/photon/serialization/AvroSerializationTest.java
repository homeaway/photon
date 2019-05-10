package com.homeaway.datatools.photon.serialization;

import com.google.common.collect.Maps;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamSchemaDao;
import com.homeaway.datatools.photon.api.schema.SchemaClient;
import com.homeaway.datatools.photon.client.PhotonClientTestHelper;
import com.homeaway.datatools.photon.client.schema.LocalSchemaClient;
import static com.homeaway.datatools.photon.client.schema.LocalSchemaClient.BEAM_SCHEMA_DAO;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import static com.homeaway.datatools.photon.serialization.AvroPhotonSerializer.AVRO_SCHEMA_CLIENT;
import lombok.Data;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.Map;
import java.util.Properties;

public class AvroSerializationTest {

    private PhotonSerializer photonSerializer;
    private PhotonDeserializer photonDeserializer;

    @Before
    public void init() {
        Properties schemaClientProperties = new Properties();
        schemaClientProperties.put(BEAM_SCHEMA_DAO, mockBeamSchemaDao());
        Map<String, Object> properties = Maps.newHashMap();
        SchemaClient schemaClient = new LocalSchemaClient();
        schemaClient.configure(schemaClientProperties);
        properties.put(AVRO_SCHEMA_CLIENT, schemaClient);
        photonSerializer = new AvroPhotonSerializer();
        photonSerializer.configure(properties);
        photonDeserializer = new AvroPhotonDeserializer();
        photonDeserializer.configure(properties);
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
