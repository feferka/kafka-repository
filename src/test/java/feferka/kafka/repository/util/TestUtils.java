package feferka.kafka.repository.util;

import lombok.val;
import org.awaitility.Awaitility;
import org.awaitility.pollinterval.FibonacciPollInterval;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import static java.util.concurrent.TimeUnit.SECONDS;

public class TestUtils {

    public static <K, V> void awaitUntilGotExpectedKey(KafkaTopicReader<K, V> reader, K key) {
        Awaitility.await()
                .pollDelay(2, SECONDS)
                .atMost(10, SECONDS)
                .pollInterval(FibonacciPollInterval.fibonacci(SECONDS))
                .until(() -> gotExpectedRecordKey(reader, key));
    }

    private static <K, V> Boolean gotExpectedRecordKey(KafkaTopicReader<K, V> reader, K key) {
        if (!reader.isEmpty()) {
            try {
                return reader.take().key().equals(key);
            } catch (InterruptedException ignored) {
            }
        }
        return false;
    }

    public static void cleanupDir(Path dir) {
        try (val stream = Files.walk(dir)) {
            stream
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException ignored) {
                        }
                    });
        } catch (IOException ignored) {
        }
    }
}
