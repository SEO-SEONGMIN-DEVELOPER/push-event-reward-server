package com.ticketing.domain.concert

import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository
import java.time.LocalDateTime

@Repository
interface ConcertRepository : JpaRepository<Concert, Long> {
    fun findByStatus(status: ConcertStatus): List<Concert>
    fun findByStartDateTimeBetween(start: LocalDateTime, end: LocalDateTime): List<Concert>
}

