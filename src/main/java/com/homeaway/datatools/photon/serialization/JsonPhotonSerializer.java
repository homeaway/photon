package com.homeaway.datatools.photon.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;

import java.util.Map;

public class JsonPhotonSerializer implements PhotonSerializer {

    private static final ObjectMapper mapper = new ObjectMapper()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .registerModule(new Jdk8Module())
            .registerModule(new JavaTimeModule());

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public byte[] serialize(PhotonBeam beam, Object payload) {
        try {
            return mapper.writeValueAsBytes(payload);
        } catch (Exception e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public void close() throws Exception {

    }
}
