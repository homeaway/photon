package com.homeaway.datatools.photon.serialization;

import com.homeaway.datatools.photon.api.schema.SchemaClient;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import static com.homeaway.datatools.photon.serialization.AvroPhotonSerializer.AVRO_SCHEMA_CLIENT;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectDatumReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

public class AvroPhotonDeserializer implements PhotonDeserializer {

    private SchemaClient schemaClient;

    @Override
    public void configure(Map<String, ?> properties) {
        schemaClient = (SchemaClient)properties.get(AVRO_SCHEMA_CLIENT);
    }

    @Override
    public <T> T deserialize(PhotonBeam beam, byte[] payload, Class<T> clazz) {
        try {
            Schema schema = schemaClient.getSchema(beam);
            DatumReader<T> datumReader = new ReflectDatumReader<>(schema);
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, new ByteArrayInputStream(payload));
            return datumReader.read(null, decoder);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public void close() throws Exception {

    }
}
