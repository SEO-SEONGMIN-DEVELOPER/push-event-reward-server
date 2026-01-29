package com.ticketing.service;

import com.ticketing.domain.concert.Concert;
import com.ticketing.domain.concert.ConcertRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Redis와 DB의 재고(remainingSeats) 동기화 서비스
 * - Consumer에서 실시간 동기화를 하지만, 예외 상황 대비 주기적 동기화 수행
 * - Redis 장애 복구 시 DB 기준으로 Redis 재구성
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class InventorySyncService {

    private final ConcertRepository concertRepository;
    private final InventoryService inventoryService;

    /**
     * DB → Redis 동기화 (주기적 실행)
     * - 매 5분마다 실행
     * - DB의 remainingSeats를 Redis에 반영
     */
    @Scheduled(fixedRate = 300000) // 5분 (300,000ms)
    @Transactional(readOnly = true)
    public void syncDbToRedis() {
        log.info("DB → Redis 재고 동기화 시작");
        
        List<Concert> concerts = concertRepository.findAll();
        int syncCount = 0;
        
        for (Concert concert : concerts) {
            Integer redisSeats = inventoryService.getRemainingSeats(concert.getId());
            Integer dbSeats = concert.getRemainingSeats();
            
            // Redis와 DB가 다르면 DB 기준으로 동기화
            if (redisSeats == null || !redisSeats.equals(dbSeats)) {
                inventoryService.syncInventory(concert.getId(), dbSeats);
                log.warn("재고 불일치 감지 및 동기화: concertId={}, Redis={}, DB={} → DB 기준 동기화",
                        concert.getId(), redisSeats, dbSeats);
                syncCount++;
            }
        }
        
        log.info("DB → Redis 재고 동기화 완료: 총 {}개 콘서트, {}개 동기화", concerts.size(), syncCount);
    }

    /**
     * Redis → DB 동기화 (수동 호출용)
     * - Redis가 정확한 상태일 때 DB를 Redis 기준으로 업데이트
     * - 주의: 트래픽이 많을 때는 호출하지 말 것
     */
    @Transactional
    public void syncRedisToDB() {
        log.info("Redis → DB 재고 동기화 시작");
        
        List<Concert> concerts = concertRepository.findAll();
        int syncCount = 0;
        
        for (Concert concert : concerts) {
            Integer redisSeats = inventoryService.getRemainingSeats(concert.getId());
            
            if (redisSeats != null && !redisSeats.equals(concert.getRemainingSeats())) {
                // Concert의 remainingSeats를 직접 업데이트할 수 있도록 setter 추가 필요
                // 또는 JPQL로 직접 UPDATE
                concertRepository.updateRemainingSeats(concert.getId(), redisSeats);
                log.info("재고 동기화: concertId={}, DB={} → Redis={}",
                        concert.getId(), concert.getRemainingSeats(), redisSeats);
                syncCount++;
            }
        }
        
        log.info("Redis → DB 재고 동기화 완료: 총 {}개 콘서트, {}개 동기화", concerts.size(), syncCount);
    }

    /**
     * 특정 콘서트의 재고 동기화 (양방향)
     * - Redis와 DB 중 어느 쪽이 정확한지 판단하여 동기화
     */
    @Transactional
    public void syncInventoryForConcert(Long concertId, String source) {
        Concert concert = concertRepository.findById(concertId)
                .orElseThrow(() -> new IllegalArgumentException("공연을 찾을 수 없습니다: " + concertId));
        
        Integer redisSeats = inventoryService.getRemainingSeats(concertId);
        Integer dbSeats = concert.getRemainingSeats();
        
        if ("redis".equalsIgnoreCase(source)) {
            // Redis 기준으로 DB 업데이트
            if (redisSeats != null) {
                concertRepository.updateRemainingSeats(concertId, redisSeats);
                log.info("Redis 기준 동기화: concertId={}, DB={} → Redis={}", 
                        concertId, dbSeats, redisSeats);
            }
        } else {
            // DB 기준으로 Redis 업데이트
            inventoryService.syncInventory(concertId, dbSeats);
            log.info("DB 기준 동기화: concertId={}, Redis={} → DB={}", 
                    concertId, redisSeats, dbSeats);
        }
    }

    /**
     * 재고 불일치 감지 및 리포트
     */
    @Transactional(readOnly = true)
    public void reportInventoryMismatch() {
        log.info("재고 불일치 감지 시작");
        
        List<Concert> concerts = concertRepository.findAll();
        int mismatchCount = 0;
        
        for (Concert concert : concerts) {
            Integer redisSeats = inventoryService.getRemainingSeats(concert.getId());
            Integer dbSeats = concert.getRemainingSeats();
            
            if (redisSeats != null && !redisSeats.equals(dbSeats)) {
                log.warn("재고 불일치: concertId={}, title={}, Redis={}, DB={}, 차이={}",
                        concert.getId(), concert.getTitle(), redisSeats, dbSeats, 
                        Math.abs(redisSeats - dbSeats));
                mismatchCount++;
            }
        }
        
        if (mismatchCount == 0) {
            log.info("재고 불일치 없음: 모든 콘서트({})의 Redis-DB 재고 일치", concerts.size());
        } else {
            log.warn("재고 불일치 감지: 총 {}개 콘서트 중 {}개 불일치", concerts.size(), mismatchCount);
        }
    }
}
