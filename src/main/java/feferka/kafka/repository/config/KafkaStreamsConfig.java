package feferka.kafka.repository.config;

import lombok.Getter;
import org.apache.kafka.streams.KafkaStreams;
import org.springframework.boot.autoconfigure.kafka.StreamsBuilderFactoryBeanCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Getter
@Configuration
public class KafkaStreamsConfig {

    private KafkaStreams.State kafkaStreamsState;

    @Bean
    public StreamsBuilderFactoryBeanCustomizer streamsBuilderFactoryBeanCustomizer() {
        return factoryBean ->
                factoryBean.setKafkaStreamsCustomizer(kafkaStreams ->
                        kafkaStreams.setStateListener((newState, oldState) ->
                                kafkaStreamsState = newState));
    }
}
