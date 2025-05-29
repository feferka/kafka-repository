package feferka.kafka.repository.util;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Slf4j
@RequiredArgsConstructor
@SuppressWarnings("unused")
public class KafkaTopicSender<K, V> {
    @Accessors(chain = true)
    @Setter
    private static Duration timeout = Duration.ofSeconds(10);
    @Getter
    private final String topic;
    private final KafkaTemplate<K, V> kafkaTemplate;

    public KafkaTopicSender(String bootStrapServers, String topic, Class<K> keyClass, Class<V> valueClass) {
        this.topic = topic;

        val keySerde = getSerdeFor(keyClass);
        val valueSerde = getSerdeFor(valueClass);

        kafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                Map.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers,
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerde.serializer().getClass(),
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerde.serializer().getClass()
                )));
    }

    public void send(K key, V value) {
        log.info("{} - sending: {} : {}", topic, key, value);
        try {
            kafkaTemplate
                    .send(topic, key, value)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            log.info("{} - sent: {} : {}", topic, key, value);
                        } else {
                            log.error("{} - failed to send {} : {}, exception:", topic, key, value, ex);
                        }
                    })
                    .get(timeout.toMillis(), MILLISECONDS);
        } catch (ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public void send(@NonNull ProducerRecord<K, V> record) {
        log.info("{} - sending: {} : {} : {}", record.topic(), record.key(), record.value(), record.headers());

        try {
            kafkaTemplate
                    .send(record)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            log.info("{} - sent: {} : {} : {}", record.topic(), record.key(), record.value(), record.headers());
                        } else {
                            log.error("{} - failed to send {} : {} : {}, exception:", record.topic(), record.key(), record.value(), record.headers(), ex);
                        }
                    })
                    .get(timeout.toMillis(), MILLISECONDS);
        } catch (ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public void close() {
        kafkaTemplate.flush();
        kafkaTemplate.destroy();
    }

    private <T> Serde<T> getSerdeFor(Class<T> clazz) {
        if (clazz == String.class) {
            //noinspection unchecked
            return (Serde<T>) Serdes.String();
        } else {
            return new JsonSerde<>(clazz);
        }
    }
}
