package feferka.kafka.repository.repository;

import lombok.RequiredArgsConstructor;
import lombok.val;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import java.util.Iterator;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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

    public V getByKey(K key) {
        return getStore().get(key);
    }

    public Stream<KeyValue<K, V>> findAll() {
        return streamKeyValues(getStore().all());
    }

    private Stream<KeyValue<K, V>> streamKeyValues(KeyValueIterator<K, V> kvIterator) {
        val kvStreamIterator = new Iterator<KeyValue<K, V>>() {
            @Override
            public boolean hasNext() {
                return kvIterator.hasNext();
            }

            @Override
            public KeyValue<K, V> next() {
                val next = kvIterator.next();
                return new KeyValue<>(next.key, next.value);
            }
        };

        val spliterator = Spliterators.spliteratorUnknownSize(kvStreamIterator, Spliterator.ORDERED);
        return StreamSupport.stream(spliterator, false).onClose(kvIterator::close);
    }

    private ReadOnlyKeyValueStore<K, V> getStore() {
        return Objects.requireNonNull(streamsBuilderFactoryBean.getKafkaStreams())
                .store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
    }
}
