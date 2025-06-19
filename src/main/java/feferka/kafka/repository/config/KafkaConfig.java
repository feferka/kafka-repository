package feferka.kafka.repository.config;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

@Getter
@EnableKafka
@EnableKafkaStreams
@Configuration
@RequiredArgsConstructor
public class KafkaConfig {

    private final KafkaStreamsConfiguration configuration;
    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    @Value(value = "${spring.kafka.topics.locations}")
    private String locationTopic;
}
