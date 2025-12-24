package com.ticketing.domain.ticket

import com.ticketing.domain.concert.Concert
import com.ticketing.domain.reservation.Reservation
import jakarta.persistence.*
import java.math.BigDecimal
import java.time.LocalDateTime

@Entity
@Table(name = "tickets")
data class Ticket(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long = 0,

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "concert_id", nullable = false)
    val concert: Concert,

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "reservation_id")
    var reservation: Reservation? = null,

    @Column(nullable = false, length = 50)
    val seatNumber: String,

    @Column(nullable = false)
    val price: BigDecimal,

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    val status: TicketStatus = TicketStatus.AVAILABLE,

    @Column(nullable = false, updatable = false)
    val createdAt: LocalDateTime = LocalDateTime.now(),

    @Column(nullable = false)
    var updatedAt: LocalDateTime = LocalDateTime.now()
) {
    fun reserve(reservation: Reservation): Boolean {
        if (status != TicketStatus.AVAILABLE) {
            return false
        }
        this.reservation = reservation
        updatedAt = LocalDateTime.now()
        return true
    }

    fun cancel() {
        this.reservation = null
        updatedAt = LocalDateTime.now()
    }
}

enum class TicketStatus {
    AVAILABLE, RESERVED, SOLD, CANCELLED
}

