package com.homeaway.datatools.photon.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;

import java.io.IOException;
import java.util.Map;

public class JsonPhotonDeserializer implements PhotonDeserializer {

    private static final ObjectMapper mapper = new ObjectMapper()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .registerModule(new Jdk8Module())
            .registerModule(new JavaTimeModule());

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public <T> T deserialize(PhotonBeam beam, byte[] payload, Class<T> clazz) {
        try {
            return mapper.readerFor(clazz).readValue(payload);
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public void close() throws Exception {

    }
}
