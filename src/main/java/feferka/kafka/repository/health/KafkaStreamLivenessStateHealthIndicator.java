package feferka.kafka.repository.health;

import feferka.kafka.repository.config.KafkaStreamsConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.springframework.boot.actuate.availability.LivenessStateHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.availability.ApplicationAvailability;
import org.springframework.stereotype.Component;

@Component("livenessStateHealthIndicator")
@Slf4j
public class KafkaStreamLivenessStateHealthIndicator extends LivenessStateHealthIndicator {

    private final KafkaStreamsConfig kafkaStreamsConfig;

    public KafkaStreamLivenessStateHealthIndicator(ApplicationAvailability availability, KafkaStreamsConfig kafkaStreamsConfig) {
        super(availability);
        this.kafkaStreamsConfig = kafkaStreamsConfig;
    }

    @Override
    protected void doHealthCheck(Health.Builder builder) {
        if (kafkaStreamsConfig.getKafkaStreamsState() == KafkaStreams.State.ERROR) {
            builder.down().withDetail("KafkaStreams", "Unavailable");
            log.error("KafkaStreams went into ERROR state");
        } else {
            builder.up().withDetail("KafkaStreams", "Available");
        }
        log.debug("KafkaStreams health checked");
    }
}
