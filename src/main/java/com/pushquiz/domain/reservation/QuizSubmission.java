package com.pushquiz.domain.reservation;

import com.pushquiz.domain.member.Member;
import com.pushquiz.domain.quiz.TimeAttackQuiz;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "reservations", indexes = {
        @Index(name = "idx_request_id", columnList = "request_id", unique = true)
})
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class QuizSubmission {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "request_id", unique = true, nullable = false, length = 36)
    private String requestId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "member_id", nullable = false)
    private Member member;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "quiz_id", nullable = false)
    private TimeAttackQuiz quiz;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private QuizSubmissionStatus status;

    public QuizSubmission(String requestId, Member member, TimeAttackQuiz quiz, QuizSubmissionStatus status) {
        this.requestId = requestId;
        this.member = member;
        this.quiz = quiz;
        this.status = status;
    }

    public void complete() {
        this.status = QuizSubmissionStatus.COMPLETED;
    }

    public void fail() {
        this.status = QuizSubmissionStatus.FAILED;
    }

    public void cancel() {
        this.status = QuizSubmissionStatus.CANCELLED;
    }
}
