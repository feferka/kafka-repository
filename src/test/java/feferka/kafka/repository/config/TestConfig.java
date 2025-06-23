package feferka.kafka.repository.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

@RequiredArgsConstructor
@TestConfiguration
public class TestConfig {

    private final KafkaConfig kafkaConfig;

    @DynamicPropertySource
    static void registerKafkaProperties(DynamicPropertyRegistry registry, KafkaContainer kafkaContainer) {
        registry.add("spring.kafka.bootstrap-servers", kafkaContainer::getBootstrapServers);
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public KafkaContainer kafkaContainer() {
        return
                new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.9")) {{
                    this.withCreateContainerCmdModifier(cmd -> cmd.withHostName("localhost"))
                            .withExposedPorts(2181, 9092, 9093).withNetwork(Network.newNetwork());
                    addEnv("CONFLUENT_SUPPORT_METRICS_ENABLE", "false");
                    setPortBindings(List.of("2181:2181", "9092:9092", "9093:9093"));
                }};
    }

    @Bean
    public NewTopic locasLocationsCompact() {
        return new NewTopic(kafkaConfig.getLocationTopic(), 3, (short) 1);
    }
}
