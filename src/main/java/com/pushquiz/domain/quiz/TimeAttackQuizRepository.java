package com.pushquiz.domain.quiz;

import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface TimeAttackQuizRepository extends JpaRepository<TimeAttackQuiz, Long> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints({
            @QueryHint(name = "jakarta.persistence.lock.timeout", value = "3000")
    })
    @Query("SELECT q FROM TimeAttackQuiz q WHERE q.id = :id")
    Optional<TimeAttackQuiz> findByIdWithLock(Long id);

    @Modifying
    @Query("UPDATE TimeAttackQuiz q SET q.remainingWinnerSlots = :remainingWinnerSlots WHERE q.id = :quizId")
    int updateRemainingWinnerSlots(Long quizId, Integer remainingWinnerSlots);
}
