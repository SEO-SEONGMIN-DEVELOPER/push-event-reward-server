package com.pushquiz.service;

import com.pushquiz.domain.member.Member;
import com.pushquiz.domain.member.MemberRepository;
import com.pushquiz.domain.quiz.TimeAttackQuiz;
import com.pushquiz.domain.quiz.TimeAttackQuizRepository;
import com.pushquiz.domain.reservation.QuizSubmissionRepository;
import com.pushquiz.facade.PushQuizFacade;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
class ReservationServiceConcurrencyTest {

    @Autowired
    private QuizSubmissionService quizSubmissionService;

    @Autowired
    private TimeAttackQuizRepository timeAttackQuizRepository;

    @Autowired
    private MemberRepository memberRepository;

    @Autowired
    private QuizSubmissionRepository quizSubmissionRepository;

    @Autowired
    private PushQuizFacade pushQuizFacade;

    private TimeAttackQuiz testQuiz;
    private List<Member> testMembers;
    private static final int TOTAL_SEATS = 100;
    private static final int CONCURRENT_USERS = 100;

    @BeforeEach
    void setUp() {
        testQuiz = new TimeAttackQuiz("테스트 퀴즈", TOTAL_SEATS, TOTAL_SEATS);
        testQuiz = timeAttackQuizRepository.save(testQuiz);

        testMembers = new ArrayList<>();
        for (int i = 0; i < CONCURRENT_USERS; i++) {
            Member member = new Member("회원" + i);
            testMembers.add(memberRepository.save(member));
        }
    }

    @AfterEach
    void tearDown() {
        quizSubmissionRepository.deleteAll();
        memberRepository.deleteAll();
        timeAttackQuizRepository.deleteAll();
    }

    @Test
    @DisplayName("동시에 100명이 100개 당첨 슬롯 퀴즈에 응모하면 레이스 컨디션으로 생성된 응모 수가 감소 슬롯 수보다 많음")
    void submit_ConcurrentSubmission_RaceConditionOccurs() throws InterruptedException {
        Long quizId = testQuiz.getId();
        ExecutorService executorService = Executors.newFixedThreadPool(CONCURRENT_USERS);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(CONCURRENT_USERS);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger illegalArgumentExceptionCount = new AtomicInteger(0);
        AtomicInteger otherExceptionCount = new AtomicInteger(0);
        Map<String, AtomicInteger> exceptionTypeCount = new ConcurrentHashMap<>();
        Map<String, String> exceptionTypeSample = new ConcurrentHashMap<>();

        for (int i = 0; i < CONCURRENT_USERS; i++) {
            final int memberIndex = i;
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    quizSubmissionService.submitWithoutLock(quizId, testMembers.get(memberIndex).getId());
                    successCount.incrementAndGet();
                } catch (IllegalArgumentException e) {
                    illegalArgumentExceptionCount.incrementAndGet();
                    String exceptionType = e.getClass().getSimpleName();
                    exceptionTypeCount.computeIfAbsent(exceptionType, k -> new AtomicInteger(0)).incrementAndGet();
                    exceptionTypeSample.putIfAbsent(exceptionType, e.getMessage());
                } catch (Exception e) {
                    otherExceptionCount.incrementAndGet();
                    String exceptionType = e.getClass().getSimpleName();
                    exceptionTypeCount.computeIfAbsent(exceptionType, k -> new AtomicInteger(0)).incrementAndGet();
                    if (!exceptionTypeSample.containsKey(exceptionType)) {
                        String message = e.getMessage();
                        if (e.getCause() != null) {
                            message += " (원인: " + e.getCause().getClass().getSimpleName() + ": " + e.getCause().getMessage() + ")";
                        }
                        exceptionTypeSample.put(exceptionType, message);
                    }
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        finishLatch.await();
        executorService.shutdown();

        TimeAttackQuiz updatedQuiz = timeAttackQuizRepository.findById(quizId).orElseThrow();
        long createdSubmissionCount = quizSubmissionRepository.count();
        int remainingWinnerSlots = updatedQuiz.getRemainingWinnerSlots();
        int decreasedWinnerSlots = TOTAL_SEATS - remainingWinnerSlots;

        assertThat(createdSubmissionCount)
                .as("레이스 컨디션으로 생성된 응모 수가 감소 슬롯 수보다 많아야 함")
                .isGreaterThan(decreasedWinnerSlots);
    }

    @Test
    @DisplayName("비관적 락 상태에서 100명 동시 응모 시 모두 성공하고 감소 슬롯 수와 일치함")
    void submit_ConcurrentWithPessimisticLock_AllSucceedAndSlotsMatch() throws InterruptedException {
        Long quizId = testQuiz.getId();
        ExecutorService executorService = Executors.newFixedThreadPool(CONCURRENT_USERS);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(CONCURRENT_USERS);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < CONCURRENT_USERS; i++) {
            final int memberIndex = i;
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    quizSubmissionService.submitWithPessimisticLock(quizId, testMembers.get(memberIndex).getId());
                    successCount.incrementAndGet();
                } catch (Exception ignored) {
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        finishLatch.await();
        executorService.shutdown();

        TimeAttackQuiz updatedQuiz = timeAttackQuizRepository.findById(quizId).orElseThrow();
        long createdSubmissionCount = quizSubmissionRepository.count();
        int remainingWinnerSlots = updatedQuiz.getRemainingWinnerSlots();
        int decreasedWinnerSlots = TOTAL_SEATS - remainingWinnerSlots;

        assertThat(createdSubmissionCount).isEqualTo(successCount.get());
        assertThat(createdSubmissionCount).isEqualTo(decreasedWinnerSlots);
    }

    @Test
    @DisplayName("분산 락 상태에서 100명 동시 응모 시 모두 성공하고 감소 슬롯 수와 일치함")
    void submit_ConcurrentWithDistributedLock_AllSucceedAndSlotsMatch() throws InterruptedException {
        Long quizId = testQuiz.getId();
        ExecutorService executorService = Executors.newFixedThreadPool(CONCURRENT_USERS);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(CONCURRENT_USERS);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < CONCURRENT_USERS; i++) {
            final int memberIndex = i;
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    pushQuizFacade.submit(quizId, testMembers.get(memberIndex).getId());
                    successCount.incrementAndGet();
                } catch (Exception ignored) {
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        finishLatch.await();
        executorService.shutdown();

        TimeAttackQuiz updatedQuiz = timeAttackQuizRepository.findById(quizId).orElseThrow();
        long createdSubmissionCount = quizSubmissionRepository.count();
        int remainingWinnerSlots = updatedQuiz.getRemainingWinnerSlots();
        int decreasedWinnerSlots = TOTAL_SEATS - remainingWinnerSlots;

        assertThat(createdSubmissionCount).isEqualTo(successCount.get());
        assertThat(createdSubmissionCount).isEqualTo(decreasedWinnerSlots);
    }
}
