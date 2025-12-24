package com.ticketing.domain.concert;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "concerts")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Concert {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 200)
    private String title;

    @Column(nullable = false)
    private Integer totalSeats;

    @Column(nullable = false)
    private Integer remainingSeats;

    public Concert(String title, Integer totalSeats, Integer remainingSeats) {
        this.title = title;
        this.totalSeats = totalSeats;
        this.remainingSeats = remainingSeats;
    }

    public void reserveSeat() {
        if (remainingSeats > 0) {
            remainingSeats--;
        }
    }
}

