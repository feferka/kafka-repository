package feferka.kafka.repository.repository;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import java.util.Objects;
import java.util.stream.Stream;

@RequiredArgsConstructor
public abstract class KafkaRepository<K, V> {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;
    private final String topicName;
    private final String storeName;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        streamsBuilder.globalTable(
                topicName,
                Materialized.<K, V, KeyValueStore<Bytes, byte[]>>as(storeName)
                        .withKeySerde(keySerde)
                        .withValueSerde(valueSerde)
        );
    }

    private ReadOnlyKeyValueStore<K, V> getStore() {
        return Objects.requireNonNull(streamsBuilderFactoryBean.getKafkaStreams())
                .store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
    }

    public V getByKey(K key) {
        return getStore().get(key);
    }

    public Stream<V> findAll() {
        return KafkaStreamUtils.streamValues(getStore().all());
    }
}
