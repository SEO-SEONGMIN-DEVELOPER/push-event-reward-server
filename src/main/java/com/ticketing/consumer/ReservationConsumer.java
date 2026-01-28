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
                // 성공 시 COMPLETED 상태로 저장
                Reservation reservation = processReservationWithRetry(event, partition, offset);
                reservation.complete();  // PENDING → COMPLETED
                reservations.add(reservation);
                successCount++;
                log.debug("예약 생성 성공: requestId={}, concertId={}, memberId={}, partition={}, offset={}",
                        event.getRequestId(), event.getConcertId(), event.getMemberId(), partition, offset);
            } catch (Exception e) {
                failureCount++;
                log.error("예약 처리 최종 실패 (3회 재시도 후): requestId={}, concertId={}, memberId={}, partition={}, offset={}, error={}",
                        event.getRequestId(), event.getConcertId(), event.getMemberId(), partition, offset, e.getMessage(), e);
                
                // 실패 시 FAILED 상태로 저장 (멤버/콘서트 조회 가능한 경우에만)
                try {
                    Member member = memberRepository.findById(event.getMemberId()).orElse(null);
                    Concert concert = concertRepository.findById(event.getConcertId()).orElse(null);
                    
                    if (member != null && concert != null) {
                        Reservation failedReservation = new Reservation(
                                event.getRequestId(), 
                                member, 
                                concert, 
                                ReservationStatus.FAILED
                        );
                        reservations.add(failedReservation);
                    }
                } catch (Exception ex) {
                    log.error("실패 예약 저장 중 오류: requestId={}, error={}", event.getRequestId(), ex.getMessage());
                }
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

    @Retryable(
            retryFor = Exception.class,           
            maxAttempts = 3,                    
            backoff = @Backoff(delay = 1000, multiplier = 2) 
    )
    public Reservation processReservationWithRetry(
            ReservationEvent event,
            Integer partition,
            Long offset
    ) {
        log.debug("예약 처리 시도: requestId={}, concertId={}, memberId={}, partition={}, offset={}",
                event.getRequestId(), event.getConcertId(), event.getMemberId(), partition, offset);

        Concert concert = concertRepository.findById(event.getConcertId())
                .orElseThrow(() -> new IllegalArgumentException(
                        String.format("공연을 찾을 수 없습니다: requestId=%s, concertId=%d, partition=%d, offset=%d",
                                event.getRequestId(), event.getConcertId(), partition, offset)));

        Member member = memberRepository.findById(event.getMemberId())
                .orElseThrow(() -> new IllegalArgumentException(
                        String.format("회원을 찾을 수 없습니다: requestId=%s, memberId=%d, partition=%d, offset=%d",
                                event.getRequestId(), event.getMemberId(), partition, offset)));

        return new Reservation(event.getRequestId(), member, concert, ReservationStatus.PENDING);
    }
}

