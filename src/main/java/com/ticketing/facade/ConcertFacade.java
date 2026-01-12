package com.ticketing.facade;

import com.ticketing.domain.concert.Concert;
import com.ticketing.domain.concert.ConcertRepository;
import com.ticketing.domain.member.Member;
import com.ticketing.domain.member.MemberRepository;
import com.ticketing.domain.reservation.Reservation;
import com.ticketing.dto.ReservationEvent;
import com.ticketing.service.InventoryService;
import com.ticketing.service.ReservationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
@Slf4j
public class ConcertFacade {

    private final RedissonClient redissonClient;
    private final ReservationService reservationService;
    private final InventoryService inventoryService;
    private final ConcertRepository concertRepository;
    private final MemberRepository memberRepository;
    private final KafkaTemplate<String, ReservationEvent> kafkaTemplate;

    private static final String LOCK_PREFIX = "lock:concert:";
    private static final long WAIT_TIME = 45;
    private static final long LEASE_TIME = 5;
    private static final String KAFKA_TOPIC = "ticket_reservation";

    public Reservation reserve(Long concertId, Long memberId) {
        String lockKey = LOCK_PREFIX + concertId;
        RLock lock = redissonClient.getLock(lockKey);

        try {
            boolean acquired = lock.tryLock(WAIT_TIME, LEASE_TIME, TimeUnit.SECONDS);
            if (!acquired) {
                throw new IllegalStateException("타임 아웃으로 인해 락 획득에 실패했습니다. 잠시 후 다시 시도해주세요.");
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

    public Reservation reserveAndHold(Long concertId, Long memberId) throws InterruptedException {
        String lockKey = LOCK_PREFIX + concertId;
        RLock lock = redissonClient.getLock(lockKey);

        try {
            boolean acquired = lock.tryLock(WAIT_TIME, LEASE_TIME, TimeUnit.SECONDS);
            if (!acquired) {
                throw new IllegalStateException("락 획득에 실패했습니다. 잠시 후 다시 시도해주세요.");
            }

            try {
                Reservation reservation = reservationService.reserveWithoutLock(concertId, memberId);
                Thread.sleep(500);
                return reservation;
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

    public Reservation reserveWithKafka(Long concertId, Long memberId) {
        String lockKey = LOCK_PREFIX + concertId;
        RLock lock = redissonClient.getLock(lockKey);

        try {
            boolean acquired = lock.tryLock(WAIT_TIME, LEASE_TIME, TimeUnit.SECONDS);
            if (!acquired) {
                throw new IllegalStateException("타임 아웃으로 인해 락 획득에 실패했습니다. 잠시 후 다시 시도해주세요.");
            }

            try {
                Member member = memberRepository.findById(memberId)
                        .orElseThrow(() -> new IllegalArgumentException("회원을 찾을 수 없습니다: " + memberId));

                Integer remainingSeats = inventoryService.getRemainingSeats(concertId);
                if (remainingSeats == null) {
                    Concert concert = concertRepository.findById(concertId)
                            .orElseThrow(() -> new IllegalArgumentException("공연을 찾을 수 없습니다: " + concertId));
                    inventoryService.initializeInventory(concertId, concert.getRemainingSeats());
                    remainingSeats = concert.getRemainingSeats();
                }

                final Integer finalRemainingSeats = remainingSeats; 

                if (finalRemainingSeats <= 0) {
                    throw new IllegalArgumentException("남은 좌석이 없습니다");
                }

                boolean decremented = inventoryService.decrementSeat(concertId);
                if (!decremented) {
                    throw new IllegalArgumentException("재고 차감에 실패했습니다. 남은 좌석이 없을 수 있습니다.");
                }

                ReservationEvent event = new ReservationEvent(concertId, memberId);
                String messageKey = String.valueOf(concertId); 
                CompletableFuture<SendResult<String, ReservationEvent>> future = 
                        kafkaTemplate.send(KAFKA_TOPIC, messageKey, event);

                final Long finalConcertId = concertId; 
                final Long finalMemberId = memberId; 
                future.whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Kafka 메시지 전송 실패: concertId={}, memberId={}", finalConcertId, finalMemberId, ex);
                        inventoryService.syncInventory(finalConcertId, finalRemainingSeats);
                    } else {
                        log.debug("Kafka 메시지 전송 성공: concertId={}, memberId={}, offset={}", 
                                finalConcertId, finalMemberId, result.getRecordMetadata().offset());
                    }
                });

                Concert concert = concertRepository.findById(concertId)
                        .orElseThrow(() -> new IllegalArgumentException("공연을 찾을 수 없습니다: " + concertId));
                
                return new Reservation(member, concert, com.ticketing.domain.reservation.ReservationStatus.PENDING);

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

