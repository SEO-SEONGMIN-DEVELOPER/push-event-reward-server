package com.ticketing.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
@Slf4j
public class InventoryService {

    private final RedisTemplate<String, String> redisTemplate;
    private static final String INVENTORY_KEY_PREFIX = "inventory:concert:";

    public Integer getRemainingSeats(Long concertId) {
        String key = INVENTORY_KEY_PREFIX + concertId;
        String value = redisTemplate.opsForValue().get(key);
        if (value == null) {
            return null;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            log.warn("Redis 재고 값 파싱 실패: concertId={}, value={}", concertId, value);
            return null;
        }
    }

    public boolean decrementSeat(Long concertId) {
        String key = INVENTORY_KEY_PREFIX + concertId;
        Long result = redisTemplate.opsForValue().decrement(key);
        
        if (result == null) {
            log.warn("Redis 재고 차감 실패: concertId={} (재고 정보가 없음)", concertId);
            return false;
        }
        
        if (result < 0) {
            redisTemplate.opsForValue().increment(key);
            log.warn("재고 부족으로 차감 실패: concertId={}, remaining={}", concertId, result);
            return false;
        }
        
        log.debug("재고 차감 성공: concertId={}, remaining={}", concertId, result);
        return true;
    }

    public void initializeInventory(Long concertId, Integer initialSeats) {
        String key = INVENTORY_KEY_PREFIX + concertId;
        redisTemplate.opsForValue().set(key, String.valueOf(initialSeats), 7, TimeUnit.DAYS);
        log.info("재고 초기화: concertId={}, seats={}", concertId, initialSeats);
    }

    public void syncInventory(Long concertId, Integer currentSeats) {
        String key = INVENTORY_KEY_PREFIX + concertId;
        redisTemplate.opsForValue().set(key, String.valueOf(currentSeats), 7, TimeUnit.DAYS);
        log.debug("재고 동기화: concertId={}, seats={}", concertId, currentSeats);
    }
}

