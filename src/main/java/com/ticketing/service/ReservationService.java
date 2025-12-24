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

@Service
@Transactional(readOnly = true)
@RequiredArgsConstructor
public class ReservationService {

    private final ReservationRepository reservationRepository;
    private final ConcertRepository concertRepository;
    private final MemberRepository memberRepository;

    @Transactional
    public Reservation reserve(Long concertId, Long memberId) {
        Concert concert = concertRepository.findById(concertId)
                .orElseThrow(() -> new IllegalArgumentException("공연을 찾을 수 없습니다: " + concertId));

        Member member = memberRepository.findById(memberId)
                .orElseThrow(() -> new IllegalArgumentException("회원을 찾을 수 없습니다: " + memberId));

        if (concert.getRemainingSeats() <= 0) {
            throw new IllegalArgumentException("남은 좌석이 없습니다");
        }

        concert.reserveSeat();

        Reservation reservation = new Reservation(member, concert, ReservationStatus.PENDING);
        return reservationRepository.save(reservation);
    }
}

