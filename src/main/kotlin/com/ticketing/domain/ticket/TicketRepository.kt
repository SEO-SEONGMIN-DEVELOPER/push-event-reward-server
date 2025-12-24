package com.ticketing.domain.ticket

import com.ticketing.domain.concert.Concert
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository

@Repository
interface TicketRepository : JpaRepository<Ticket, Long> {
    fun findByConcert(concert: Concert): List<Ticket>
    fun findByConcertAndStatus(concert: Concert, status: TicketStatus): List<Ticket>
}

