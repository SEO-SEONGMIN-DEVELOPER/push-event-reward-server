package com.ticketing.domain.reservation

import com.ticketing.domain.member.Member
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository
import java.util.Optional

@Repository
interface ReservationRepository : JpaRepository<Reservation, Long> {
    fun findByMember(member: Member): List<Reservation>
    fun findByReservationNumber(reservationNumber: String): Optional<Reservation>
    fun findByStatus(status: ReservationStatus): List<Reservation>
}

