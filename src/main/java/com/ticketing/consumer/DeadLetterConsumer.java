package com.ticketing.consumer;

import com.ticketing.dto.DeadLetterEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class DeadLetterConsumer {

    @KafkaListener(
            topics = "ticket_reservation_dlq",
            groupId = "ticketing-dlq-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeDeadLetterEvents(
            @Payload List<DeadLetterEvent> events,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) List<Integer> partitions,
            @Header(KafkaHeaders.OFFSET) List<Long> offsets,
            Acknowledgment acknowledgment
    ) {
        log.warn("=== DLQ 메시지 수신 ===");
        log.warn("Topic: {}, Batch Size: {}", topic, events.size());

        for (int i = 0; i < events.size(); i++) {
            DeadLetterEvent event = events.get(i);
            Integer partition = partitions.get(i);
            Long offset = offsets.get(i);

            log.error("DLQ 메시지 상세:");
            log.error("  - RequestId: {}", event.getOriginalEvent().getRequestId());
            log.error("  - ConcertId: {}", event.getOriginalEvent().getConcertId());
            log.error("  - MemberId: {}", event.getOriginalEvent().getMemberId());
            log.error("  - Error Type: {}", event.getErrorType());
            log.error("  - Error Message: {}", event.getErrorMessage());
            log.error("  - Failed At: {}", event.getFailedAt());
            log.error("  - Retry Attempts: {}", event.getRetryAttempts());
            log.error("  - Original Partition: {}, Offset: {}", event.getPartition(), event.getOffset());
            log.error("  - DLQ Partition: {}, DLQ Offset: {}", partition, offset);
            log.error("=====================================");
        }

        if (acknowledgment != null) {
            acknowledgment.acknowledge();
        }
    }
}
