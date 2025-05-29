package feferka.kafka.repository.config;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.Map;

@Getter
@EnableKafka
@EnableKafkaStreams
@Configuration
@RequiredArgsConstructor
public class KafkaConfig {

    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value(value = "${spring.kafka.topics.locations}")
    private String locationTopic;

    private final KafkaStreamsConfiguration configuration;

    @PostConstruct
    void init() {
        configuration
                .asProperties()
                .putAll(Map.of(
                                StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, KafkaStreamsErrorHandlers.CustomDeserializationExceptionHandler.class,
                                StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, KafkaStreamsErrorHandlers.CustomProcessingExceptionHandler.class,
                                StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, KafkaStreamsErrorHandlers.CustomProductionExceptionHandler.class
                        )
                );
    }
}
