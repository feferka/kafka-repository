package feferka.kafka.repository.config;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.apache.kafka.streams.processor.api.Record;

import java.util.Map;

public class KafkaStreamsErrorHandlers {
    @Slf4j
    public static class CustomDeserializationExceptionHandler implements DeserializationExceptionHandler {

        @Override
        public void configure(Map<String, ?> configs) {
        }

        @Override
        public DeserializationHandlerResponse handle(ErrorHandlerContext context, ConsumerRecord<byte[], byte[]> record, Exception exception) {
            val isCausedByKafka = exception.getCause() instanceof KafkaException;
            log.error("""
                    Deserialization error
                        isCausedByKafka: {}
                        topic: {}
                        partition: {}
                        offset: {}
                        exception: {}
                        cause: {}
                    """, isCausedByKafka, context.topic(), context.partition(), context.offset(), getDescription(exception), getDescription(exception.getCause()));

            return DeserializationHandlerResponse.CONTINUE;
        }
    }

    @Slf4j
    public static class CustomProcessingExceptionHandler implements ProcessingExceptionHandler {

        @Override
        public void configure(Map<String, ?> configs) {
        }

        @Override
        public ProcessingHandlerResponse handle(ErrorHandlerContext context, Record<?, ?> record, Exception exception) {
            log.error("""
                    Processing error
                        topic: {}
                        partition: {}
                        offset: {}
                        exception: {}
                        cause: {}
                    """, context.topic(), context.partition(), context.offset(), getDescription(exception), getDescription(exception.getCause()));

            return ProcessingHandlerResponse.CONTINUE;
        }
    }

    @Slf4j
    public static class CustomProductionExceptionHandler implements ProductionExceptionHandler {

        @Override
        public void configure(Map<String, ?> configs) {
        }

        @Override
        public ProductionExceptionHandlerResponse handle(ErrorHandlerContext context, ProducerRecord<byte[], byte[]> record, Exception exception) {
            val retriable = exception instanceof RetriableException;
            log.error("""
                    Production error
                        retriable: {}
                        topic: {}
                        partition: {}
                        offset: {}
                        exception: {}
                        cause: {}
                    """, retriable, context.topic(), context.partition(), context.offset(), getDescription(exception), getDescription(exception.getCause()));

            return retriable ? ProductionExceptionHandlerResponse.CONTINUE : ProductionExceptionHandlerResponse.FAIL;
        }
    }

    private static String getDescription(Throwable throwable) {
        return throwable == null
                ? ""
                : throwable.getClass().getSimpleName() + " - " + throwable.getMessage();
    }
}
