package feferka.kafka.repository.util;

import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.jetbrains.annotations.NotNull;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
public class KafkaTopicReader<K, V> {

    private final BlockingQueue<ConsumerRecord<K, V>> output;
    private final KafkaMessageListenerContainer<K, V> consumer;

    @Accessors(fluent = true)
    @Getter
    private long totalCount;

    public long count() {
        return output.size();
    }

    public KafkaTopicReader(String bootstrapServers, String topic, Class<K> keyClass, Class<V> valueClass) {
        output = new LinkedBlockingQueue<>();
        totalCount = 0;
        consumer = new KafkaMessageListenerContainer<>(getConsumerFactory(bootstrapServers, keyClass, valueClass), new ContainerProperties(topic)) {{
            setBeanName(topic);
            setupMessageListener((MessageListener<K, V>) record -> {
                log.info("{} - received: {} : {}", topic, record.key(), record.value());
                output.add(record);
                totalCount++;
                log.info("{} - totalCount: {}, count:{}", topic, totalCount, output.size());
            });
        }};
        consumer.start();
    }

    private @NotNull DefaultKafkaConsumerFactory<K, V> getConsumerFactory(
            String bootstrapServers,
            Class<K> keyClass,
            Class<V> valueClass
    ) {
        val keySerde = getSerdeFor(keyClass);
        val valueSerde = getSerdeFor(valueClass);

        val config = new java.util.HashMap<String, Object>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keySerde.getClass().getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueSerde.getClass().getName());

        if (keySerde.deserializer() instanceof JsonDeserializer) {
            config.put(JsonDeserializer.KEY_DEFAULT_TYPE, keyClass.getName());
            config.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        }

        if (valueSerde.deserializer() instanceof JsonDeserializer) {
            config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, valueClass.getName());
            config.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        }

        return new DefaultKafkaConsumerFactory<>(config, keySerde.deserializer(), valueSerde.deserializer());
    }

    public boolean isEmpty() {
        return output.isEmpty();
    }

    public ConsumerRecord<K, V> take() throws InterruptedException {
        return output.take();
    }

    public void close() {
        consumer.stop();
        totalCount = 0;
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
