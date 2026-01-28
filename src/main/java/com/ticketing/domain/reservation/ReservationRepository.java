package com.ticketing.domain.reservation;

import com.ticketing.domain.member.Member;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface ReservationRepository extends JpaRepository<Reservation, Long> {
    List<Reservation> findByMember(Member member);

    List<Reservation> findByStatus(ReservationStatus status);

    Optional<Reservation> findByRequestId(String requestId);
}

