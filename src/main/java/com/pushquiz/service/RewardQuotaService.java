package com.pushquiz.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
@Slf4j
public class RewardQuotaService {

    private final RedisTemplate<String, String> redisTemplate;
    private static final String REWARD_QUOTA_KEY_PREFIX = "inventory:quiz:";

    public Integer getRemainingWinnerSlots(Long quizId) {
        String key = REWARD_QUOTA_KEY_PREFIX + quizId;
        String value = redisTemplate.opsForValue().get(key);
        if (value == null) {
            return null;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            log.warn("Redis 리워드 수량 파싱 실패: quizId={}, value={}", quizId, value);
            return null;
        }
    }

    public boolean decrementWinnerSlot(Long quizId) {
        String key = REWARD_QUOTA_KEY_PREFIX + quizId;
        Long result = redisTemplate.opsForValue().decrement(key);

        if (result == null) {
            log.warn("Redis 리워드 차감 실패: quizId={} (수량 정보가 없음)", quizId);
            return false;
        }

        if (result < 0) {
            redisTemplate.opsForValue().increment(key);
            log.warn("리워드 수량 부족으로 차감 실패: quizId={}, remaining={}", quizId, result);
            return false;
        }

        log.debug("리워드 차감 성공: quizId={}, remaining={}", quizId, result);
        return true;
    }

    public void incrementWinnerSlot(Long quizId) {
        String key = REWARD_QUOTA_KEY_PREFIX + quizId;
        Long result = redisTemplate.opsForValue().increment(key);
        log.info("리워드 수량 복구: quizId={}, remaining={}", quizId, result);
    }

    public void initializeRewardQuota(Long quizId, Integer initialWinnerSlots) {
        String key = REWARD_QUOTA_KEY_PREFIX + quizId;
        redisTemplate.opsForValue().set(key, String.valueOf(initialWinnerSlots), 7, TimeUnit.DAYS);
        log.info("리워드 수량 초기화: quizId={}, slots={}", quizId, initialWinnerSlots);
    }

    public void syncRewardQuota(Long quizId, Integer currentWinnerSlots) {
        String key = REWARD_QUOTA_KEY_PREFIX + quizId;
        redisTemplate.opsForValue().set(key, String.valueOf(currentWinnerSlots), 7, TimeUnit.DAYS);
        log.debug("리워드 수량 동기화: quizId={}, slots={}", quizId, currentWinnerSlots);
    }
}
