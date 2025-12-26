package com.ticketing.facade;

import com.ticketing.domain.reservation.Reservation;
import com.ticketing.service.ReservationService;
import lombok.RequiredArgsConstructor;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
public class ConcertFacade {

    private final RedissonClient redissonClient;
    private final ReservationService reservationService;

    private static final String LOCK_PREFIX = "lock:concert:";
    private static final long WAIT_TIME = 5;
    private static final long LEASE_TIME = 3;

    public Reservation reserve(Long concertId, Long memberId) {
        String lockKey = LOCK_PREFIX + concertId;
        RLock lock = redissonClient.getLock(lockKey);

        try {
            boolean acquired = lock.tryLock(WAIT_TIME, LEASE_TIME, TimeUnit.SECONDS);
            if (!acquired) {
                throw new IllegalStateException("락 획득에 실패했습니다. 잠시 후 다시 시도해주세요.");
            }

            try {
                return reservationService.reserveWithoutLock(concertId, memberId);
            } finally {
                if (lock.isHeldByCurrentThread()) {
                    lock.unlock();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("락 획득 중 인터럽트가 발생했습니다.", e);
        }
    }
}

