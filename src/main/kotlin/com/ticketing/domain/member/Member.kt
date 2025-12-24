package com.ticketing.domain.member

import jakarta.persistence.*
import java.time.LocalDateTime

@Entity
@Table(name = "members")
data class Member(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long = 0,

    @Column(nullable = false, unique = true, length = 50)
    val email: String,

    @Column(nullable = false, length = 100)
    var password: String,

    @Column(nullable = false, length = 50)
    val name: String,

    @Column(nullable = false, length = 20)
    val phoneNumber: String,

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    val status: MemberStatus = MemberStatus.ACTIVE,

    @Column(nullable = false, updatable = false)
    val createdAt: LocalDateTime = LocalDateTime.now(),

    @Column(nullable = false)
    var updatedAt: LocalDateTime = LocalDateTime.now()
) {
    fun updatePassword(newPassword: String) {
        this.password = newPassword
        this.updatedAt = LocalDateTime.now()
    }

    fun deactivate() {
        // 상태 변경 로직은 setter 대신 메서드로 처리
    }
}

enum class MemberStatus {
    ACTIVE, INACTIVE, SUSPENDED
}

