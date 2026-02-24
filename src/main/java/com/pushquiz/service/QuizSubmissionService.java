package com.pushquiz.service;

import com.pushquiz.domain.member.Member;
import com.pushquiz.domain.member.MemberRepository;
import com.pushquiz.domain.quiz.TimeAttackQuiz;
import com.pushquiz.domain.quiz.TimeAttackQuizRepository;
import com.pushquiz.domain.reservation.QuizSubmission;
import com.pushquiz.domain.reservation.QuizSubmissionRepository;
import com.pushquiz.domain.reservation.QuizSubmissionStatus;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
@Transactional(readOnly = true)
@RequiredArgsConstructor
public class QuizSubmissionService {

    private final QuizSubmissionRepository quizSubmissionRepository;
    private final TimeAttackQuizRepository timeAttackQuizRepository;
    private final MemberRepository memberRepository;

    @Transactional
    public QuizSubmission submitWithPessimisticLock(Long quizId, Long memberId) {
        TimeAttackQuiz quiz = timeAttackQuizRepository.findByIdWithLock(quizId)
                .orElseThrow(() -> new IllegalArgumentException("퀴즈를 찾을 수 없습니다: " + quizId));

        Member member = memberRepository.findById(memberId)
                .orElseThrow(() -> new IllegalArgumentException("회원을 찾을 수 없습니다: " + memberId));

        if (quiz.getRemainingWinnerSlots() <= 0) {
            throw new IllegalArgumentException("남은 선착순 보상이 없습니다");
        }

        quiz.claimWinnerSlot();

        String requestId = UUID.randomUUID().toString();
        QuizSubmission submission = new QuizSubmission(requestId, member, quiz, QuizSubmissionStatus.COMPLETED);
        return quizSubmissionRepository.save(submission);
    }

    @Transactional
    public QuizSubmission submitWithoutLock(Long quizId, Long memberId) {
        TimeAttackQuiz quiz = timeAttackQuizRepository.findById(quizId)
                .orElseThrow(() -> new IllegalArgumentException("퀴즈를 찾을 수 없습니다: " + quizId));

        Member member = memberRepository.findById(memberId)
                .orElseThrow(() -> new IllegalArgumentException("회원을 찾을 수 없습니다: " + memberId));

        if (quiz.getRemainingWinnerSlots() <= 0) {
            throw new IllegalArgumentException("남은 선착순 보상이 없습니다");
        }

        quiz.claimWinnerSlot();

        try {
            Thread.sleep(1800);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("처리 중 인터럽트가 발생했습니다.", e);
        }

        String requestId = UUID.randomUUID().toString();
        QuizSubmission submission = new QuizSubmission(requestId, member, quiz, QuizSubmissionStatus.COMPLETED);
        return quizSubmissionRepository.save(submission);
    }

    @Transactional
    public QuizSubmission submitWithPessimisticLockAndHold(Long quizId, Long memberId) throws InterruptedException {
        TimeAttackQuiz quiz = timeAttackQuizRepository.findByIdWithLock(quizId)
                .orElseThrow(() -> new IllegalArgumentException("퀴즈를 찾을 수 없습니다: " + quizId));

        Thread.sleep(500);

        Member member = memberRepository.findById(memberId)
                .orElseThrow(() -> new IllegalArgumentException("회원을 찾을 수 없습니다: " + memberId));

        if (quiz.getRemainingWinnerSlots() <= 0) {
            throw new IllegalArgumentException("남은 선착순 보상이 없습니다");
        }

        quiz.claimWinnerSlot();

        String requestId = UUID.randomUUID().toString();
        QuizSubmission submission = new QuizSubmission(requestId, member, quiz, QuizSubmissionStatus.COMPLETED);
        return quizSubmissionRepository.save(submission);
    }
}
