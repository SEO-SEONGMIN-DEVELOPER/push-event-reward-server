package com.ticketing.service;

import com.ticketing.domain.concert.Concert;
import com.ticketing.domain.concert.ConcertRepository;
import com.ticketing.domain.member.Member;
import com.ticketing.domain.member.MemberRepository;
import com.ticketing.domain.reservation.Reservation;
import com.ticketing.domain.reservation.ReservationRepository;
import com.ticketing.domain.reservation.ReservationStatus;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
@Transactional(readOnly = true)
@RequiredArgsConstructor
public class ReservationService {

    private final ReservationRepository reservationRepository;
    private final ConcertRepository concertRepository;
    private final MemberRepository memberRepository;

    @Transactional
    public Reservation reserveWithPessimisticLock(Long concertId, Long memberId) {
        Concert concert = concertRepository.findByIdWithLock(concertId)
                .orElseThrow(() -> new IllegalArgumentException("공연을 찾을 수 없습니다: " + concertId));

        Member member = memberRepository.findById(memberId)
                .orElseThrow(() -> new IllegalArgumentException("회원을 찾을 수 없습니다: " + memberId));

        if (concert.getRemainingSeats() <= 0) {
            throw new IllegalArgumentException("남은 좌석이 없습니다");
        }

        concert.reserveSeat();

        String requestId = UUID.randomUUID().toString();
        Reservation reservation = new Reservation(requestId, member, concert, ReservationStatus.COMPLETED);
        return reservationRepository.save(reservation);
    }

    @Transactional
    public Reservation reserveWithoutLock(Long concertId, Long memberId) {
        Concert concert = concertRepository.findById(concertId)
                .orElseThrow(() -> new IllegalArgumentException("공연을 찾을 수 없습니다: " + concertId));

        Member member = memberRepository.findById(memberId)
                .orElseThrow(() -> new IllegalArgumentException("회원을 찾을 수 없습니다: " + memberId));

        if (concert.getRemainingSeats() <= 0) {
            throw new IllegalArgumentException("남은 좌석이 없습니다");
        }

        concert.reserveSeat();

        try {
            Thread.sleep(1800);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("처리 중 인터럽트가 발생했습니다.", e);
        }

        String requestId = UUID.randomUUID().toString();
        Reservation reservation = new Reservation(requestId, member, concert, ReservationStatus.COMPLETED);
        return reservationRepository.save(reservation);
    }

    @Transactional
    public Reservation reserveWithPessimisticLockAndHold(Long concertId, Long memberId) throws InterruptedException {
        Concert concert = concertRepository.findByIdWithLock(concertId)
                .orElseThrow(() -> new IllegalArgumentException("공연을 찾을 수 없습니다: " + concertId));

        // 락을 획득한 상태에서 0.5초 대기 (커넥션 점유 시간 증가)
        Thread.sleep(500);

        Member member = memberRepository.findById(memberId)
                .orElseThrow(() -> new IllegalArgumentException("회원을 찾을 수 없습니다: " + memberId));

        if (concert.getRemainingSeats() <= 0) {
            throw new IllegalArgumentException("남은 좌석이 없습니다");
        }

        concert.reserveSeat();

        String requestId = UUID.randomUUID().toString();
        Reservation reservation = new Reservation(requestId, member, concert, ReservationStatus.COMPLETED);
        return reservationRepository.save(reservation);
    }
}