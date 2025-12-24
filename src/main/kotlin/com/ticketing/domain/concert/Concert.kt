package com.ticketing.domain.concert

import jakarta.persistence.*
import java.math.BigDecimal
import java.time.LocalDateTime

@Entity
@Table(name = "concerts")
data class Concert(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long = 0,

    @Column(nullable = false, length = 200)
    val title: String,

    @Column(nullable = false, length = 1000)
    val description: String,

    @Column(nullable = false)
    val venue: String,

    @Column(nullable = false)
    val totalSeats: Int,

    @Column(nullable = false)
    var availableSeats: Int,

    @Column(nullable = false)
    val ticketPrice: BigDecimal,

    @Column(nullable = false)
    val startDateTime: LocalDateTime,

    @Column(nullable = false)
    val endDateTime: LocalDateTime,

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    val status: ConcertStatus = ConcertStatus.UPCOMING,

    @Column(nullable = false, updatable = false)
    val createdAt: LocalDateTime = LocalDateTime.now(),

    @Column(nullable = false)
    var updatedAt: LocalDateTime = LocalDateTime.now()
) {
    fun reserveSeats(count: Int): Boolean {
        if (availableSeats < count) {
            return false
        }
        availableSeats -= count
        updatedAt = LocalDateTime.now()
        return true
    }

    fun cancelSeats(count: Int) {
        availableSeats += count
        updatedAt = LocalDateTime.now()
    }

    fun isAvailable(): Boolean {
        return status == ConcertStatus.UPCOMING && availableSeats > 0
    }
}

enum class ConcertStatus {
    UPCOMING, ONGOING, COMPLETED, CANCELLED
}

