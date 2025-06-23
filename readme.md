## kafka-repository

#### Motivation:

To access a Kafka topic like a collection.

Internally uses GlobalKTable which is persisted as RockDB in local file system in `state-dir` directory.

#### Usage:

```java
    @Repository
    public class LocationRepository extends KafkaRepository<String, Location> {
    
        public LocationRepository(StreamsBuilderFactoryBean streamsBuilderFactoryBean, KafkaConfig kafkaConfig) {
            super(streamsBuilderFactoryBean,
                    kafkaConfig.getLocationTopic(),
                    "locations-store",
                    Serdes.String(),
                    new JsonSerde<>(Location.class));
        }
    }
```

KafkaRepository will provide two basic methods:

```java
    public V getByKey(K key);

    public Stream<KeyValue<K, V>> findAll();
```

Kafka user doesn't need to have rights to manage topics.<br>
( It doesn't create Kafka streams internal topics or changelogs ... )

