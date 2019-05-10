package com.homeaway.datatools.photon.serialization;

import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;

import java.util.Map;

public interface PhotonDeserializer extends AutoCloseable {

    void configure(Map<String, ?> properties);

    <T> T deserialize(PhotonBeam beam, byte[] payload, Class<T> clazz);
}
