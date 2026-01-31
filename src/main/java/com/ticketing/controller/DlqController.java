package com.ticketing.controller;

import com.ticketing.dto.DeadLetterEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/dlq")
@RequiredArgsConstructor
@Slf4j
public class DlqController {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @PostMapping("/retry")
    public ResponseEntity<Map<String, Object>> retryDeadLetterMessage(
            @RequestBody DeadLetterEvent dlqEvent
    ) {
        try {
            String originalTopic = "ticket_reservation";
            kafkaTemplate.send(originalTopic, dlqEvent.getOriginalEvent().getRequestId(), dlqEvent.getOriginalEvent());
            
            log.info("DLQ 메시지 재전송 완료: requestId={}, topic={}", 
                    dlqEvent.getOriginalEvent().getRequestId(), originalTopic);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("requestId", dlqEvent.getOriginalEvent().getRequestId());
            response.put("retriedTopic", originalTopic);
            response.put("message", "DLQ 메시지가 원본 토픽으로 재전송되었습니다");
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("DLQ 재전송 실패: requestId={}, error={}", 
                    dlqEvent.getOriginalEvent().getRequestId(), e.getMessage(), e);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("requestId", dlqEvent.getOriginalEvent().getRequestId());
            response.put("error", e.getMessage());
            
            return ResponseEntity.internalServerError().body(response);
        }
    }

    @GetMapping("/info")
    public ResponseEntity<Map<String, Object>> getDlqInfo() {
        Map<String, Object> info = new HashMap<>();
        info.put("dlqTopic", "ticket_reservation_dlq");
        info.put("consumerGroup", "ticketing-dlq-group");
        info.put("description", "재시도 3회 실패한 예약 요청이 저장됩니다");
        info.put("retryEndpoint", "POST /api/dlq/retry");
        
        return ResponseEntity.ok(info);
    }
}
