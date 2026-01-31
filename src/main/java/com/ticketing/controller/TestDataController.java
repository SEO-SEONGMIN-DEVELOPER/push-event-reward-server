package com.ticketing.controller;

import com.ticketing.domain.concert.Concert;
import com.ticketing.domain.concert.ConcertRepository;
import com.ticketing.domain.member.Member;
import com.ticketing.domain.member.MemberRepository;
import com.ticketing.domain.reservation.ReservationRepository;
import com.ticketing.service.InventoryService;
import com.ticketing.service.InventorySyncService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/test")
@RequiredArgsConstructor
public class TestDataController {

    private final ReservationRepository reservationRepository;
    private final ConcertRepository concertRepository;
    private final MemberRepository memberRepository;
    private final InventoryService inventoryService;
    private final InventorySyncService inventorySyncService;
    private final org.springframework.kafka.core.KafkaTemplate<String, Object> kafkaTemplate;

    @PostMapping("/init")
    @Transactional
    public ResponseEntity<InitResponse> initializeTestData() {
        reservationRepository.deleteAll();
        concertRepository.deleteAll();
        memberRepository.deleteAll();

        List<Concert> concerts = new ArrayList<>();
        concerts.add(new Concert("2024 봄 콘서트", 100000, 10000));
        concerts.add(new Concert("2024 여름 페스티벌", 100000, 10000));
        concerts.add(new Concert("2024 가을 뮤지컬", 100000, 10000));
        concerts.add(new Concert("2024 겨울 오케스트라", 100000, 10000));
        concerts.add(new Concert("2024 연말 갈라쇼", 100000, 10000));
        concerts = concertRepository.saveAll(concerts);

        for (Concert concert : concerts) {
            inventoryService.initializeInventory(concert.getId(), 100000);
        }

        List<Member> members = new ArrayList<>();
        for (int i = 1; i <= 200; i++) {
            members.add(new Member(String.format("회원%03d", i)));
        }
        members = memberRepository.saveAll(members);

        List<Long> concertIds = concerts.stream()
                .map(Concert::getId)
                .toList();
        
        List<Long> memberIds = members.stream()
                .map(Member::getId)
                .toList();

        return ResponseEntity.status(HttpStatus.CREATED)
                .body(new InitResponse(
                        concerts.size(),
                        members.size(),
                        concertIds,
                        memberIds,
                        "테스트 데이터가 성공적으로 초기화되었습니다."
                ));
    }

    @PostMapping("/sync/db-to-redis")
    public ResponseEntity<SyncResponse> syncDbToRedis() {
        inventorySyncService.syncDbToRedis();
        return ResponseEntity.ok(new SyncResponse("DB → Redis 재고 동기화가 완료되었습니다."));
    }

    @PostMapping("/sync/redis-to-db")
    public ResponseEntity<SyncResponse> syncRedisToDB() {
        inventorySyncService.syncRedisToDB();
        return ResponseEntity.ok(new SyncResponse("Redis → DB 재고 동기화가 완료되었습니다."));
    }

    @PostMapping("/sync/concert/{concertId}")
    public ResponseEntity<SyncResponse> syncConcertInventory(
            @PathVariable Long concertId,
            @RequestParam(defaultValue = "db") String source) {
        inventorySyncService.syncInventoryForConcert(concertId, source);
        return ResponseEntity.ok(new SyncResponse(
                String.format("콘서트 %d의 재고가 %s 기준으로 동기화되었습니다.", concertId, source.toUpperCase())
        ));
    }

    @GetMapping("/sync/report")
    public ResponseEntity<SyncResponse> reportInventoryMismatch() {
        inventorySyncService.reportInventoryMismatch();
        return ResponseEntity.ok(new SyncResponse("재고 불일치 리포트가 로그에 출력되었습니다."));
    }

    @GetMapping("/inventory/{concertId}")
    public ResponseEntity<InventoryStatusResponse> getInventoryStatus(@PathVariable Long concertId) {
        Concert concert = concertRepository.findById(concertId)
                .orElseThrow(() -> new IllegalArgumentException("공연을 찾을 수 없습니다: " + concertId));
        
        Integer redisSeats = inventoryService.getRemainingSeats(concertId);
        Integer dbSeats = concert.getRemainingSeats();
        boolean synced = redisSeats != null && redisSeats.equals(dbSeats);
        
        return ResponseEntity.ok(new InventoryStatusResponse(
                concertId,
                concert.getTitle(),
                redisSeats,
                dbSeats,
                synced,
                synced ? 0 : Math.abs((redisSeats != null ? redisSeats : 0) - dbSeats)
        ));
    }

    public record InitResponse(
            int concertCount,
            int memberCount,
            List<Long> concertIds,
            List<Long> memberIds,
            String message
    ) {
    }

    public record SyncResponse(String message) {
    }

    public record InventoryStatusResponse(
            Long concertId,
            String title,
            Integer redisSeats,
            Integer dbSeats,
            boolean synced,
            int difference
    ) {
    }

    @PostMapping("/force-dlq")
    public ResponseEntity<Map<String, String>> forceDlqTest() {
        com.ticketing.dto.ReservationEvent event = new com.ticketing.dto.ReservationEvent(99999L, 99999L);
        kafkaTemplate.send("ticket_reservation", event.getRequestId(), event);
        
        Map<String, String> response = new HashMap<>();
        response.put("message", "DLQ 테스트 이벤트 전송 완료");
        response.put("requestId", event.getRequestId());
        response.put("concertId", "99999");
        response.put("memberId", "99999");
        response.put("info", "약 7초 후 DLQ 로그를 확인하세요");
        
        return ResponseEntity.ok(response);
    }
}

