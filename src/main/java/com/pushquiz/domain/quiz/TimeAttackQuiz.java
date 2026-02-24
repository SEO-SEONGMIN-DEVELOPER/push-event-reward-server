package com.pushquiz.domain.quiz;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "quizzes")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class TimeAttackQuiz {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 200)
    private String title;

    @Column(nullable = false)
    private Integer totalWinnerSlots;

    @Column(nullable = false)
    private Integer remainingWinnerSlots;

    public TimeAttackQuiz(String title, Integer totalWinnerSlots, Integer remainingWinnerSlots) {
        this.title = title;
        this.totalWinnerSlots = totalWinnerSlots;
        this.remainingWinnerSlots = remainingWinnerSlots;
    }

    public void claimWinnerSlot() {
        if (remainingWinnerSlots > 0) {
            remainingWinnerSlots--;
        }
    }
}
