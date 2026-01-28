package com.ticketing.domain.reservation;

import com.ticketing.domain.concert.Concert;
import com.ticketing.domain.member.Member;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "reservations", indexes = {
        @Index(name = "idx_request_id", columnList = "request_id", unique = true)
})
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Reservation {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "request_id", unique = true, nullable = false, length = 36)
    private String requestId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "member_id", nullable = false)
    private Member member;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "concert_id", nullable = false)
    private Concert concert;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ReservationStatus status;

    public Reservation(String requestId, Member member, Concert concert, ReservationStatus status) {
        this.requestId = requestId;
        this.member = member;
        this.concert = concert;
        this.status = status;
    }

    // 상태 업데이트 메서드
    public void complete() {
        this.status = ReservationStatus.COMPLETED;
    }

    public void fail() {
        this.status = ReservationStatus.FAILED;
    }

    public void cancel() {
        this.status = ReservationStatus.CANCELLED;
    }
}

