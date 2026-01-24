package com.ticketing.consumer;

import com.ticketing.domain.concert.Concert;
import com.ticketing.domain.concert.ConcertRepository;
import com.ticketing.domain.member.Member;
import com.ticketing.domain.member.MemberRepository;
import com.ticketing.domain.reservation.Reservation;
import com.ticketing.domain.reservation.ReservationRepository;
import com.ticketing.domain.reservation.ReservationStatus;
import com.ticketing.dto.ReservationEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class ReservationConsumer {

    private final ReservationRepository reservationRepository;
    private final ConcertRepository concertRepository;
    private final MemberRepository memberRepository;

    @KafkaListener(
            topics = "ticket_reservation",
            groupId = "ticketing-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional
    public void consumeReservationEvents(
            @Payload List<ReservationEvent> events,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) List<Integer> partitions,
            @Header(KafkaHeaders.OFFSET) List<Long> offsets,
            Acknowledgment acknowledgment
    ) {
        log.info("예약 이벤트 수신: topic={}, batchSize={}", topic, events.size());

        List<Reservation> reservations = new ArrayList<>();
        int successCount = 0;
        int failureCount = 0;

        for (int i = 0; i < events.size(); i++) {
            ReservationEvent event = events.get(i);
            Integer partition = partitions.get(i);
            Long offset = offsets.get(i);

            try {
                Reservation reservation = processReservationWithRetry(event, partition, offset);
                reservations.add(reservation);
                successCount++;
                log.debug("예약 생성 성공: concertId={}, memberId={}, partition={}, offset={}",
                        event.getConcertId(), event.getMemberId(), partition, offset);
            } catch (Exception e) {
                failureCount++;
                log.error("예약 처리 최종 실패 (3회 재시도 후): concertId={}, memberId={}, partition={}, offset={}, error={}",
                        event.getConcertId(), event.getMemberId(), partition, offset, e.getMessage(), e);
            }
        }

        if (!reservations.isEmpty()) {
            reservationRepository.saveAll(reservations);
            log.debug("예약 DB 배치 저장 완료: count={}", reservations.size());
        }

        log.info("예약 이벤트 처리 완료: 성공={}, 실패={}, 총={}", successCount, failureCount, events.size());

        if (acknowledgment != null) {
            acknowledgment.acknowledge();
        }
    }

    /**
     * 개별 예약 처리 (재시도 가능)
     * 
     * [재시도 정책]
     * - maxAttempts: 최대 3회 시도 (최초 1회 + 재시도 2회)
     * - backoff: 재시도 간격 1초에서 시작하여 2배씩 증가 (1초 → 2초)
     * - 총 소요 시간: 최대 약 3초 (1초 대기 + 2초 대기)
     * 
     * [재시도되는 예외]
     * - Exception.class: 모든 예외에 대해 재시도
     * - 일시적 오류 (DB 연결 실패, 타임아웃 등)
     * 
     * [재시도 흐름]
     * 1차 시도 실패 → 1초 대기 → 2차 시도 실패 → 2초 대기 → 3차 시도 실패 → 예외 throw
     * 
     * @param event 예약 이벤트
     * @param partition 파티션 번호
     * @param offset Offset
     * @return 생성된 Reservation 엔티티
     * @throws Exception 3회 재시도 후에도 실패 시
     */
    @Retryable(
            retryFor = Exception.class,           // 모든 예외에 대해 재시도
            maxAttempts = 3,                       // 최대 3회 시도
            backoff = @Backoff(delay = 1000, multiplier = 2)  // 1초 대기, 다음은 2배씩 증가
    )
    public Reservation processReservationWithRetry(
            ReservationEvent event,
            Integer partition,
            Long offset
    ) {
        log.debug("예약 처리 시도: concertId={}, memberId={}, partition={}, offset={}",
                event.getConcertId(), event.getMemberId(), partition, offset);

        // 공연 조회 (없으면 예외 발생 → 재시도)
        Concert concert = concertRepository.findById(event.getConcertId())
                .orElseThrow(() -> new IllegalArgumentException(
                        String.format("공연을 찾을 수 없습니다: concertId=%d, partition=%d, offset=%d",
                                event.getConcertId(), partition, offset)));

        // 회원 조회 (없으면 예외 발생 → 재시도)
        Member member = memberRepository.findById(event.getMemberId())
                .orElseThrow(() -> new IllegalArgumentException(
                        String.format("회원을 찾을 수 없습니다: memberId=%d, partition=%d, offset=%d",
                                event.getMemberId(), partition, offset)));

        // 예약 엔티티 생성 및 반환
        return new Reservation(member, concert, ReservationStatus.PENDING);
    }
}

