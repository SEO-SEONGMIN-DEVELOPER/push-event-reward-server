package com.ticketing.domain.reservation

import com.ticketing.domain.concert.Concert
import com.ticketing.domain.member.Member
import com.ticketing.domain.ticket.Ticket
import jakarta.persistence.*
import java.math.BigDecimal
import java.time.LocalDateTime

@Entity
@Table(name = "reservations")
data class Reservation(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long = 0,

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "member_id", nullable = false)
    val member: Member,

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "concert_id", nullable = false)
    val concert: Concert,

    @OneToMany(mappedBy = "reservation", cascade = [CascadeType.ALL], orphanRemoval = true)
    val tickets: MutableList<Ticket> = mutableListOf(),

    @Column(nullable = false)
    val ticketCount: Int,

    @Column(nullable = false)
    val totalAmount: BigDecimal,

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    val status: ReservationStatus = ReservationStatus.PENDING,

    @Column(nullable = false, unique = true, length = 50)
    val reservationNumber: String,

    @Column(nullable = false, updatable = false)
    val createdAt: LocalDateTime = LocalDateTime.now(),

    @Column(nullable = false)
    var updatedAt: LocalDateTime = LocalDateTime.now(),

    @Column
    var expiredAt: LocalDateTime? = null
) {
    fun confirm() {
        // 상태는 외부에서 변경
    }

    fun cancel() {
        // 취소 로직
    }

    fun isExpired(): Boolean {
        return expiredAt != null && expiredAt!!.isBefore(LocalDateTime.now())
    }
}

enum class ReservationStatus {
    PENDING, CONFIRMED, CANCELLED, EXPIRED
}

