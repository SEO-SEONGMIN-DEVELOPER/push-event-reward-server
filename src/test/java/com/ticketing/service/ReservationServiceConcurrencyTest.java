package com.ticketing.service;

import com.ticketing.domain.concert.Concert;
import com.ticketing.domain.concert.ConcertRepository;
import com.ticketing.domain.member.Member;
import com.ticketing.domain.member.MemberRepository;
import com.ticketing.domain.reservation.ReservationRepository;
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
    private ReservationService reservationService;

    @Autowired
    private ConcertRepository concertRepository;

    @Autowired
    private MemberRepository memberRepository;

    @Autowired
    private ReservationRepository reservationRepository;

    private Concert testConcert;
    private List<Member> testMembers;
    private static final int TOTAL_SEATS = 100;
    private static final int CONCURRENT_USERS = 100;

    @BeforeEach
    void setUp() {
        testConcert = new Concert("테스트 공연", TOTAL_SEATS, TOTAL_SEATS);
        testConcert = concertRepository.save(testConcert);

        testMembers = new ArrayList<>();
        for (int i = 0; i < CONCURRENT_USERS; i++) {
            Member member = new Member("회원" + i);
            testMembers.add(memberRepository.save(member));
        }
    }

    @AfterEach
    void tearDown() {
        reservationRepository.deleteAll();
        memberRepository.deleteAll();
        concertRepository.deleteAll();
    }

    @Test
    @DisplayName("동시에 100명이 100석 공연을 예약하면 레이스 컨디션으로 인해 생성된 예약 수가 감소된 좌석 수보다 많다")
    void reserve_ConcurrentReservation_RaceConditionOccurs() throws InterruptedException {
        Long concertId = testConcert.getId();
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
                    reservationService.reserve(concertId, testMembers.get(memberIndex).getId());
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

        Concert updatedConcert = concertRepository.findById(concertId)
                .orElseThrow();

        long createdReservationCount = reservationRepository.count();
        int remainingSeats = updatedConcert.getRemainingSeats();
        int decreasedSeats = TOTAL_SEATS - remainingSeats;

        int totalAttempts = successCount.get() + illegalArgumentExceptionCount.get() + otherExceptionCount.get();
        
        System.out.println("\n========== 테스트 결과 ==========");
        System.out.println("성공한 예약 수: " + successCount.get());
        System.out.println("실패한 예약 수: " + (illegalArgumentExceptionCount.get() + otherExceptionCount.get()));
        System.out.println("  - IllegalArgumentException: " + illegalArgumentExceptionCount.get());
        System.out.println("  - 기타 예외: " + otherExceptionCount.get());
        System.out.println("\n[예외 타입별 통계 및 샘플]");
        exceptionTypeCount.forEach((type, count) -> {
            System.out.println("  - " + type + ": " + count.get() + "개");
            String sample = exceptionTypeSample.get(type);
            if (sample != null) {
                System.out.println("    샘플: " + sample);
            }
        });
        System.out.println("\n총 시도 수: " + totalAttempts);
        System.out.println("초기 좌석 수: " + TOTAL_SEATS);
        System.out.println("남은 좌석 수: " + remainingSeats);
        System.out.println("감소된 좌석 수: " + decreasedSeats);
        System.out.println("생성된 예약 수: " + createdReservationCount);
        System.out.println("=================================\n");

        assertThat(createdReservationCount)
                .as("레이스 컨디션으로 인해 생성된 예약 수가 감소된 좌석 수보다 많아야 함")
                .isGreaterThan(decreasedSeats);
    }
}

