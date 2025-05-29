package feferka.kafka.repository;

import feferka.kafka.repository.config.KafkaConfig;
import feferka.kafka.repository.model.location.Gps;
import feferka.kafka.repository.model.location.Location;
import feferka.kafka.repository.repository.LocationRepository;
import feferka.kafka.repository.util.KafkaTopicReader;
import feferka.kafka.repository.util.KafkaTopicSender;
import feferka.kafka.repository.util.TestUtils;
import lombok.val;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.nio.file.Path;

@DihTest
public class LocationRepositoryTest {

    @Value("${spring.kafka.streams.state-dir}")
    private Path tempDir;

    @Autowired
    private KafkaConfig kafkaConfig;

    @Autowired
    private LocationRepository locationsRepository;

    @AutoClose
    private KafkaTopicSender<String, Location> locationSender;

    @AutoClose
    private KafkaTopicReader<String, Location> locationReader;

    @BeforeEach
    void beforeEach() {
        locationSender = new KafkaTopicSender<>(kafkaConfig.getBootstrapServers(), kafkaConfig.getLocationTopic(), String.class, Location.class);
        locationReader = new KafkaTopicReader<>(kafkaConfig.getBootstrapServers(), kafkaConfig.getLocationTopic(), String.class, Location.class);
    }

    @AfterEach
    void afterEach() {
        TestUtils.cleanupDir(tempDir);
    }

    @DisplayName("produced location should appears in both Kafka repository and Kafka reader")
    @Test
    void providerMappingBeforeLocationTest() {
        // given
        val id = "id";
        val location = new Location()
                .withName("Hotel Srdce Beskyd")
                .withAddress("Čeladná 461, 73912 Čeladná, Česko")
                .withGps(new Gps().withLat("49.4745850N").withLon("18.3595728E"));

        // when
        {
            locationSender.send(id, location);
        }
        // then
        {
            TestUtils.awaitUntilGotExpectedKey(locationReader, id);
            val locationByKey = locationsRepository.getByKey(id);
            val locations = locationsRepository.findAll().toList();

            Assertions.assertNotNull(locationByKey);
            Assertions.assertEquals(1, locations.size());

            Assertions.assertEquals(location.getName(), locationByKey.getName());
            Assertions.assertEquals(location.getAddress(), locationByKey.getAddress());
            Assertions.assertEquals(location.getGps(), locationByKey.getGps());

            Assertions.assertEquals(location.getName(), locations.getFirst().getName());
            Assertions.assertEquals(location.getAddress(), locations.getFirst().getAddress());
            Assertions.assertEquals(location.getGps(), locations.getFirst().getGps());
        }
    }
}
