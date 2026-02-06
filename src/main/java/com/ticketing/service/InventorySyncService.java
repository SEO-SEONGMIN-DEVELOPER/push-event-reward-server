package com.ticketing.service;

import com.ticketing.domain.concert.Concert;
import com.ticketing.domain.concert.ConcertRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class InventorySyncService {

    private final ConcertRepository concertRepository;
    private final InventoryService inventoryService;

    @Scheduled(fixedRate = 300000)
    @Transactional(readOnly = true)
    public void syncDbToRedis() {
        log.info("DB → Redis 재고 동기화 시작");
        
        List<Concert> concerts = concertRepository.findAll();
        int syncCount = 0;
        
        for (Concert concert : concerts) {
            Integer redisSeats = inventoryService.getRemainingSeats(concert.getId());
            Integer dbSeats = concert.getRemainingSeats();
            
            if (redisSeats == null || !redisSeats.equals(dbSeats)) {
                inventoryService.syncInventory(concert.getId(), dbSeats);
                log.warn("재고 불일치 감지 및 동기화: concertId={}, Redis={}, DB={} → DB 기준 동기화",
                        concert.getId(), redisSeats, dbSeats);
                syncCount++;
            }
        }
        
        log.info("DB → Redis 재고 동기화 완료: 총 {}개 콘서트, {}개 동기화", concerts.size(), syncCount);
    }

    @Transactional
    public void syncRedisToDB() {
        log.info("Redis → DB 재고 동기화 시작");
        
        List<Concert> concerts = concertRepository.findAll();
        int syncCount = 0;
        
        for (Concert concert : concerts) {
            Integer redisSeats = inventoryService.getRemainingSeats(concert.getId());
            
            if (redisSeats != null && !redisSeats.equals(concert.getRemainingSeats())) {
                concertRepository.updateRemainingSeats(concert.getId(), redisSeats);
                log.info("재고 동기화: concertId={}, DB={} → Redis={}",
                        concert.getId(), concert.getRemainingSeats(), redisSeats);
                syncCount++;
            }
        }
        
        log.info("Redis → DB 재고 동기화 완료: 총 {}개 콘서트, {}개 동기화", concerts.size(), syncCount);
    }

    @Transactional
    public void syncInventoryForConcert(Long concertId, String source) {
        Concert concert = concertRepository.findById(concertId)
                .orElseThrow(() -> new IllegalArgumentException("공연을 찾을 수 없습니다: " + concertId));
        
        Integer redisSeats = inventoryService.getRemainingSeats(concertId);
        Integer dbSeats = concert.getRemainingSeats();
        
        if ("redis".equalsIgnoreCase(source)) {
            if (redisSeats != null) {
                concertRepository.updateRemainingSeats(concertId, redisSeats);
                log.info("Redis 기준 동기화: concertId={}, DB={} → Redis={}", 
                        concertId, dbSeats, redisSeats);
            }
        } else {
            inventoryService.syncInventory(concertId, dbSeats);
            log.info("DB 기준 동기화: concertId={}, Redis={} → DB={}", 
                    concertId, redisSeats, dbSeats);
        }
    }

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
