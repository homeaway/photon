package com.homeaway.datatools.photon.client.consumer.partition.consumer;

import com.google.common.collect.Lists;
import com.homeaway.datatools.photon.api.beam.BeamException;
import com.homeaway.datatools.photon.api.beam.BeamProducer;
import com.homeaway.datatools.photon.api.beam.PhotonMessage;
import com.homeaway.datatools.photon.api.beam.PhotonMessageHandler;
import com.homeaway.datatools.photon.api.schema.SchemaClient;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamDao;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamDataDao;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamManifestDao;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamProcessedDao;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamReaderDao;
import static com.homeaway.datatools.photon.PhotonClientMockHelper.mockBeamSchemaDao;
import static com.homeaway.datatools.photon.client.PhotonClientTestHelper.buildPhotonBeamReader;
import com.homeaway.datatools.photon.client.cache.BeamCache;
import com.homeaway.datatools.photon.client.cache.BeamReaderCache;
import com.homeaway.datatools.photon.client.cache.DefaultBeamCache;
import com.homeaway.datatools.photon.client.cache.DefaultBeamReaderCache;
import com.homeaway.datatools.photon.client.consumer.PhotonBeamReaderConfig;
import com.homeaway.datatools.photon.client.producer.DefaultBeamProducer;
import com.homeaway.datatools.photon.client.schema.LocalSchemaClient;
import static com.homeaway.datatools.photon.client.schema.LocalSchemaClient.BEAM_SCHEMA_DAO;
import com.homeaway.datatools.photon.dao.beam.BeamDao;
import com.homeaway.datatools.photon.dao.beam.BeamDataDao;
import com.homeaway.datatools.photon.dao.beam.BeamDataManifestDao;
import com.homeaway.datatools.photon.dao.beam.BeamProcessedDao;
import com.homeaway.datatools.photon.dao.beam.BeamReaderDao;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReader;
import com.homeaway.datatools.photon.dao.model.beam.PhotonBeamReaderLock;
import static com.homeaway.datatools.photon.api.model.enums.PhotonBeamReaderOffsetType.FROM_CURRENT;
import com.homeaway.datatools.photon.serialization.JsonPhotonDeserializer;
import com.homeaway.datatools.photon.serialization.JsonPhotonSerializer;
import com.homeaway.datatools.photon.utils.consumer.BasePartitionConsumer;
import com.homeaway.datatools.photon.utils.dao.DefaultPartitionHelper;
import com.homeaway.datatools.photon.utils.dao.PartitionHelper;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class DefaultWalkBackBeamConsumerTest {

    private BeamDao beamDao;
    private BeamCache beamCache;
    private BeamReaderDao beamReaderDao;
    private BeamReaderCache beamReaderCache;
    private BeamDataDao beamDataDao;
    private BeamDataManifestDao beamDataManifestDao;
    private BeamProcessedDao beamProcessedDao;
    private PartitionHelper partitionHelper;
    private WalkBackBeamConsumer walkBackBeamConsumer;
    private SchemaClient schemaClient;

    @Before
    public void init() {
        beamDao = mockBeamDao();
        beamCache = new DefaultBeamCache(beamDao);
        beamReaderDao = mockBeamReaderDao();
        beamReaderCache = new DefaultBeamReaderCache(beamReaderDao);
        partitionHelper = new DefaultPartitionHelper(10);
        beamDataManifestDao = mockBeamManifestDao(partitionHelper);
        beamDataDao = mockBeamDataDao(beamDataManifestDao, partitionHelper);
        beamProcessedDao = mockBeamProcessedDao();
        Properties properties = new Properties();
        properties.put(BEAM_SCHEMA_DAO, mockBeamSchemaDao());
        schemaClient = new LocalSchemaClient();
        schemaClient.configure(properties);
        walkBackBeamConsumer =
                new DefaultWalkBackBeamConsumer(new JsonPhotonDeserializer(), beamCache, beamReaderCache, beamDataDao, beamDataManifestDao, beamProcessedDao,
                        new BasePartitionConsumer<>(10), new BasePartitionConsumer<>(50), new BasePartitionConsumer<>(100),
                        partitionHelper);
    }

    @Test
    public void testConsume() throws Exception {
        List<PhotonMessage> messageList = Lists.newArrayList();
        String beamName = "testBeamName";
        BeamProducer producer = new DefaultBeamProducer(beamDataDao, beamCache, new JsonPhotonSerializer());
        for (int i = 0; i < 10; i++) {
            producer.writeMessageToBeam(beamName, UUID.randomUUID().toString(),
                    new PayloadClass(1, UUID.randomUUID(), "This is the message"), Instant.now().minusSeconds(2000 * i));
        }

        beamCache.getBeamByName(beamName).peek().setStartDate(Instant.now().minusSeconds(20000));
        beamCache.getBeamByUuid(beamCache.getBeamByName(beamName).peek().getBeamUuid()).get().setStartDate(Instant.now().minusSeconds(20000));
        PhotonBeamReader beamReader = buildPhotonBeamReader();
        beamReader.setBeamUuid(beamCache.getBeamByName(beamName).peek().getBeamUuid());
        beamReader.setPhotonBeamReaderLock(new PhotonBeamReaderLock(UUID.randomUUID(), Instant.now()));
        beamReaderCache.putPhotonBeamReader(beamCache.getBeamByName(beamName).peek(), beamReader);

        PhotonBeamReaderConfig config = new PhotonBeamReaderConfig(beamReader.getClientName(), beamName, new PhotonMessageHandler() {
            @Override
            public void handleMessage(PhotonMessage message) {
            }

            @Override
            public void handleException(BeamException beamException) {

            }

            @Override
            public void handleStaleMessage(PhotonMessage message) {
                messageList.add(message);
            }

        }, FROM_CURRENT, (client, beam) -> Instant.now());

        walkBackBeamConsumer.putBeamForWalking(config, Duration.ofSeconds(20000));
        walkBackBeamConsumer.start();

        Thread.sleep(2000);

        walkBackBeamConsumer.stop();

        Assert.assertEquals(10, messageList.size());
    }

    @Data
    @AllArgsConstructor
    private static class PayloadClass {
        private Integer Id;
        private UUID uuid;
        private String message;
    }
}
