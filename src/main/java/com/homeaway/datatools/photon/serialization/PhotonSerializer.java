package com.homeaway.datatools.photon.serialization;

import com.homeaway.datatools.photon.dao.model.beam.PhotonBeam;

import java.util.Map;

public interface PhotonSerializer extends AutoCloseable {

    void configure(Map<String, ?> properties);

    byte[] serialize(PhotonBeam beam, Object payload);

}
