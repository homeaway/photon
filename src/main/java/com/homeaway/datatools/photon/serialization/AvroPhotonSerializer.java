package com.homeaway.datatools.photon.serialization;

import com.google.common.collect.Maps;
import com.homeaway.datatools.photon.api.schema.SchemaClient;
import com.homeaway.datatools.photon.api.schema.SchemaException;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class AvroPhotonSerializer implements PhotonSerializer {

    public static final String AVRO_SCHEMA_CLIENT = "avro.schema.client";

    private static final ConcurrentMap<Class<?>, Schema> schemas = Maps.newConcurrentMap();
    private SchemaClient schemaClient;

    @Override
    public void configure(Map<String, ?> properties) {
        schemaClient = (SchemaClient)properties.get(AVRO_SCHEMA_CLIENT);
    }

    @Override
    public byte[] serialize(PhotonBeam beam, Object payload) {

        try {
            schemaClient.registerSchema(beam, getObjectSchema(payload));
            Schema schema = schemaClient.getSchema(beam);
            DatumWriter<Object> writer = new ReflectDatumWriter<>(schema);
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            Encoder jsonEncoder = EncoderFactory.get().jsonEncoder(schema, stream);
            writer.write(payload, jsonEncoder);
            jsonEncoder.flush();
            return stream.toByteArray();

        } catch (SchemaException | IOException e) {
            log.error("Schema compatibility exception logging message", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {

    }

    private Schema getObjectSchema(Object object) {
        return schemas.computeIfAbsent(object.getClass(), o -> {
            if (o.isAssignableFrom(GenericContainer.class)) {
                return ((GenericContainer)object).getSchema();
            } else {
                return ReflectData.get().getSchema(o);
            }
        });
    }
}
