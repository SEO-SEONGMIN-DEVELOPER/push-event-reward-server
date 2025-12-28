package com.ticketing.service;

import com.ticketing.domain.concert.Concert;
import com.ticketing.domain.concert.ConcertRepository;
import com.ticketing.domain.member.Member;
import com.ticketing.domain.member.MemberRepository;
import com.ticketing.domain.reservation.ReservationRepository;
import com.ticketing.facade.ConcertFacade;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = {
        "spring.datasource.hikari.maximum-pool-size=10",
        "spring.datasource.hikari.connection-timeout=250"
})

class ConnectionStarvationTest {

    @Autowired
    private ReservationService reservationService;

    @Autowired
    private ConcertRepository concertRepository;

    @Autowired
    private MemberRepository memberRepository;

    @Autowired
    private ReservationRepository reservationRepository;

    @Autowired
    private ConcertFacade concertFacade;

    private Concert testConcert;
    private List<Member> testMembers;
    private static final int TOTAL_SEATS = 100;
    private static final int ATTACK_GROUP_SIZE = 30;
    private static final int VICTIM_GROUP_SIZE = 10;

    @BeforeEach
    void setUp() {
        testConcert = new Concert("테스트 공연", TOTAL_SEATS, TOTAL_SEATS);
        testConcert = concertRepository.save(testConcert);

        testMembers = new ArrayList<>();
        for (int i = 0; i < ATTACK_GROUP_SIZE + VICTIM_GROUP_SIZE; i++) {
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
    @DisplayName("비관적 락 사용 시 DB 커넥션 고갈로 인해 다른 요청이 실패함")
    void test_PessimisticLock_Starvation() throws InterruptedException {
        Long concertId = testConcert.getId();
        ExecutorService executorService = Executors.newFixedThreadPool(ATTACK_GROUP_SIZE + VICTIM_GROUP_SIZE);
        CountDownLatch attackStartLatch = new CountDownLatch(1);
        CountDownLatch attackFinishLatch = new CountDownLatch(ATTACK_GROUP_SIZE);
        CountDownLatch victimStartLatch = new CountDownLatch(1);
        CountDownLatch victimFinishLatch = new CountDownLatch(VICTIM_GROUP_SIZE);

        AtomicInteger attackSuccessCount = new AtomicInteger(0);
        AtomicInteger attackFailureCount = new AtomicInteger(0);
        AtomicInteger victimSuccessCount = new AtomicInteger(0);
        AtomicInteger victimFailureCount = new AtomicInteger(0);

        // 공격조: 비관적 락으로 예매 시도 (커넥션 점유)
        for (int i = 0; i < ATTACK_GROUP_SIZE; i++) {
            final int memberIndex = i;
            executorService.submit(() -> {
                try {
                    attackStartLatch.await();
                    reserveWithPessimisticLockAndHold(concertId, testMembers.get(memberIndex).getId());
                    attackSuccessCount.incrementAndGet();
                } catch (Exception e) {
                    attackFailureCount.incrementAndGet();
                } finally {
                    attackFinishLatch.countDown();
                }
            });
        }

        // 피해자조: 단순 회원 조회
        for (int i = 0; i < VICTIM_GROUP_SIZE; i++) {
            final int memberIndex = ATTACK_GROUP_SIZE + i;
            executorService.submit(() -> {
                try {
                    victimStartLatch.await();
                    memberRepository.findById(testMembers.get(memberIndex).getId())
                            .orElseThrow(() -> new IllegalArgumentException("회원을 찾을 수 없습니다"));
                    victimSuccessCount.incrementAndGet();
                } catch (Exception e) {
                    victimFailureCount.incrementAndGet();
                } finally {
                    victimFinishLatch.countDown();
                }
            });
        }

        attackStartLatch.countDown();
        
        Thread.sleep(100);
        victimStartLatch.countDown();

        attackFinishLatch.await();
        victimFinishLatch.await();

        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.SECONDS);

        System.out.println("\n=========================================");
        System.out.println("[안정성 테스트 결과 - 비관적 락]");
        System.out.println("- 커넥션 풀 크기: 10개");
        System.out.println("- 예매 시도(공격): " + ATTACK_GROUP_SIZE + "명");
        System.out.println("- 단순 조회(피해자): " + VICTIM_GROUP_SIZE + "명");
        System.out.println("-----------------------------------------");
        System.out.println("- 단순 조회 성공: " + victimSuccessCount.get() + " 명");
        System.out.println("- 단순 조회 실패: " + victimFailureCount.get() + " 명 (Connection Timeout 등)");
        System.out.println("=========================================\n");

        assertThat(victimFailureCount.get())
                .as("비관적 락 사용 시 커넥션 고갈로 인해 피해자조 대부분이 실패해야 함")
                .isGreaterThan(VICTIM_GROUP_SIZE / 2);
    }

    private void reserveWithPessimisticLockAndHold(Long concertId, Long memberId) throws InterruptedException {
        reservationService.reserveWithPessimisticLockAndHold(concertId, memberId);
    }

    private void reserveWithDistributedLockAndHold(Long concertId, Long memberId) throws InterruptedException {
        concertFacade.reserveAndHold(concertId, memberId);
    }

    @Test
    @DisplayName("분산 락 사용 시 DB 커넥션 고갈 없이 다른 요청이 정상 처리됨")
    void test_DistributedLock_Starvation() throws InterruptedException {
        Long concertId = testConcert.getId();
        ExecutorService executorService = Executors.newFixedThreadPool(ATTACK_GROUP_SIZE + VICTIM_GROUP_SIZE);
        CountDownLatch attackStartLatch = new CountDownLatch(1);
        CountDownLatch attackFinishLatch = new CountDownLatch(ATTACK_GROUP_SIZE);
        CountDownLatch victimStartLatch = new CountDownLatch(1);
        CountDownLatch victimFinishLatch = new CountDownLatch(VICTIM_GROUP_SIZE);

        AtomicInteger attackSuccessCount = new AtomicInteger(0);
        AtomicInteger attackFailureCount = new AtomicInteger(0);
        AtomicInteger victimSuccessCount = new AtomicInteger(0);
        AtomicInteger victimFailureCount = new AtomicInteger(0);

        // 공격조: 분산 락으로 예매 시도
        for (int i = 0; i < ATTACK_GROUP_SIZE; i++) {
            final int memberIndex = i;
            executorService.submit(() -> {
                try {
                    attackStartLatch.await();
                    reserveWithDistributedLockAndHold(concertId, testMembers.get(memberIndex).getId());
                    attackSuccessCount.incrementAndGet();
                } catch (Exception e) {
                    attackFailureCount.incrementAndGet();
                } finally {
                    attackFinishLatch.countDown();
                }
            });
        }

        // 피해자조: 단순 회원 조회
        for (int i = 0; i < VICTIM_GROUP_SIZE; i++) {
            final int memberIndex = ATTACK_GROUP_SIZE + i;
            executorService.submit(() -> {
                try {
                    victimStartLatch.await();
                    memberRepository.findById(testMembers.get(memberIndex).getId())
                            .orElseThrow(() -> new IllegalArgumentException("회원을 찾을 수 없습니다"));
                    victimSuccessCount.incrementAndGet();
                } catch (Exception e) {
                    victimFailureCount.incrementAndGet();
                } finally {
                    victimFinishLatch.countDown();
                }
            });
        }

        
        attackStartLatch.countDown();
        
        Thread.sleep(100);
        victimStartLatch.countDown();

        attackFinishLatch.await();
        victimFinishLatch.await();

        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.SECONDS);

        System.out.println("\n=========================================");
        System.out.println("[안정성 테스트 결과 - 분산 락]");
        System.out.println("- 커넥션 풀 크기: 10개");
        System.out.println("- 예매 시도(공격): " + ATTACK_GROUP_SIZE + "명");
        System.out.println("- 단순 조회(피해자): " + VICTIM_GROUP_SIZE + "명");
        System.out.println("-----------------------------------------");
        System.out.println("- 단순 조회 성공: " + victimSuccessCount.get() + " 명");
        System.out.println("- 단순 조회 실패: " + victimFailureCount.get() + " 명 (Connection Timeout 등)");
        System.out.println("=========================================\n");

        assertThat(victimSuccessCount.get())
                .as("분산 락 사용 시 커넥션 고갈 없이 피해자조 모두 성공해야 함")
                .isEqualTo(VICTIM_GROUP_SIZE);
    }
}

