package feferka.kafka.repository.repository;

import feferka.kafka.repository.config.KafkaConfig;
import feferka.kafka.repository.model.location.Location;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Repository;

@Repository
public class LocationRepository extends KafkaRepository<String, Location> {

    public LocationRepository(StreamsBuilderFactoryBean streamsBuilderFactoryBean, KafkaConfig kafkaConfig) {
        super(streamsBuilderFactoryBean,
                kafkaConfig.getLocasLocationsTopic(),
                "locations-store",
                Serdes.String(),
                new JsonSerde<>(Location.class));
    }
}
