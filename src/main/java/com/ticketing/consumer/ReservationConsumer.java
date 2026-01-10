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
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

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

        int successCount = 0;
        int failureCount = 0;

        for (int i = 0; i < events.size(); i++) {
            ReservationEvent event = events.get(i);
            Integer partition = partitions.get(i);
            Long offset = offsets.get(i);

            try {
                processReservationEvent(event, partition, offset);
                successCount++;
                log.debug("예약 처리 성공: concertId={}, memberId={}, partition={}, offset={}", 
                        event.getConcertId(), event.getMemberId(), partition, offset);
            } catch (Exception e) {
                failureCount++;
                log.error("예약 처리 실패: concertId={}, memberId={}, partition={}, offset={}, error={}", 
                        event.getConcertId(), event.getMemberId(), partition, offset, e.getMessage(), e);
            }
        }

        log.info("예약 이벤트 처리 완료: 성공={}, 실패={}, 총={}", successCount, failureCount, events.size());

        if (acknowledgment != null) {
            acknowledgment.acknowledge();
        }
    }

    private void processReservationEvent(ReservationEvent event, Integer partition, Long offset) {
        Long concertId = event.getConcertId();
        Long memberId = event.getMemberId();

        Concert concert = concertRepository.findById(concertId)
                .orElseThrow(() -> new IllegalArgumentException(
                        String.format("공연을 찾을 수 없습니다: concertId=%d, partition=%d, offset=%d", 
                                concertId, partition, offset)));

        Member member = memberRepository.findById(memberId)
                .orElseThrow(() -> new IllegalArgumentException(
                        String.format("회원을 찾을 수 없습니다: memberId=%d, partition=%d, offset=%d", 
                                memberId, partition, offset)));

        Reservation reservation = new Reservation(member, concert, ReservationStatus.PENDING);
        Reservation savedReservation = reservationRepository.save(reservation);

        log.debug("예약 DB 저장 완료: reservationId={}, concertId={}, memberId={}", 
                savedReservation.getId(), concertId, memberId);
    }
}

