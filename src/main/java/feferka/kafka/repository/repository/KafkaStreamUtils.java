package feferka.kafka.repository.repository;

import lombok.val;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class KafkaStreamUtils {

    public static <K, V> Stream<V> streamValues(KeyValueIterator<K, V> kvIterator) {
        val valueIterator = new Iterator<V>() {
            @Override
            public boolean hasNext() {
                return kvIterator.hasNext();
            }

            @Override
            public V next() {
                return kvIterator.next().value;
            }
        };

        val spliterator = Spliterators.spliteratorUnknownSize(valueIterator, Spliterator.ORDERED);

        return StreamSupport
                .stream(spliterator, false)
                .onClose(kvIterator::close);
    }
}
