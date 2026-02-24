package com.pushquiz.facade;

import com.pushquiz.domain.member.MemberRepository;
import com.pushquiz.domain.quiz.TimeAttackQuizRepository;
import com.pushquiz.domain.reservation.QuizSubmission;
import com.pushquiz.domain.reservation.QuizSubmissionRepository;
import com.pushquiz.dto.QuizSubmissionEvent;
import com.pushquiz.service.QuizSubmissionService;
import com.pushquiz.service.RewardQuotaService;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class PushQuizFacade {

    private final RedissonClient redissonClient;
    private final QuizSubmissionService quizSubmissionService;
    private final RewardQuotaService rewardQuotaService;
    private final TimeAttackQuizRepository timeAttackQuizRepository;
    private final MemberRepository memberRepository;
    private final QuizSubmissionRepository quizSubmissionRepository;

    @Autowired(required = false)
    private KafkaTemplate<String, QuizSubmissionEvent> kafkaTemplate;

    public PushQuizFacade(RedissonClient redissonClient,
                          QuizSubmissionService quizSubmissionService,
                          RewardQuotaService rewardQuotaService,
                          TimeAttackQuizRepository timeAttackQuizRepository,
                          MemberRepository memberRepository,
                          QuizSubmissionRepository quizSubmissionRepository) {
        this.redissonClient = redissonClient;
        this.quizSubmissionService = quizSubmissionService;
        this.rewardQuotaService = rewardQuotaService;
        this.timeAttackQuizRepository = timeAttackQuizRepository;
        this.memberRepository = memberRepository;
        this.quizSubmissionRepository = quizSubmissionRepository;
    }

    private static final String LOCK_PREFIX = "lock:quiz:";
    private static final long WAIT_TIME = 2;
    private static final long LEASE_TIME = 1;
    private static final String KAFKA_TOPIC = "quiz_submission";

    public QuizSubmission submit(Long quizId, Long memberId) {
        String lockKey = LOCK_PREFIX + quizId;
        RLock lock = redissonClient.getLock(lockKey);

        try {
            boolean acquired = lock.tryLock(WAIT_TIME, LEASE_TIME, TimeUnit.SECONDS);
            if (!acquired) {
                throw new IllegalStateException("타임 아웃으로 인해 락 획득에 실패했습니다. 잠시 후 다시 시도해주세요.");
            }

            try {
                return quizSubmissionService.submitWithoutLock(quizId, memberId);
            } finally {
                if (lock.isHeldByCurrentThread()) {
                    lock.unlock();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("락 획득 중 인터럽트가 발생했습니다.", e);
        }
    }

    public QuizSubmission submitAndHold(Long quizId, Long memberId) throws InterruptedException {
        String lockKey = LOCK_PREFIX + quizId;
        RLock lock = redissonClient.getLock(lockKey);

        try {
            boolean acquired = lock.tryLock(WAIT_TIME, LEASE_TIME, TimeUnit.SECONDS);
            if (!acquired) {
                throw new IllegalStateException("락 획득에 실패했습니다. 잠시 후 다시 시도해주세요.");
            }

            try {
                QuizSubmission submission = quizSubmissionService.submitWithoutLock(quizId, memberId);
                Thread.sleep(500);
                return submission;
            } finally {
                if (lock.isHeldByCurrentThread()) {
                    lock.unlock();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("락 획득 중 인터럽트가 발생했습니다.", e);
        }
    }

    public String submitWithKafka(Long quizId, Long memberId) {
        boolean decremented = rewardQuotaService.decrementWinnerSlot(quizId);
        if (!decremented) {
            throw new IllegalArgumentException("리워드 차감에 실패했습니다. 남은 선착순 보상이 없을 수 있습니다.");
        }

        QuizSubmissionEvent event = new QuizSubmissionEvent(quizId, memberId);
        String messageKey = String.valueOf(quizId);
        CompletableFuture<SendResult<String, QuizSubmissionEvent>> future =
                kafkaTemplate.send(KAFKA_TOPIC, messageKey, event);

        final String requestId = event.getRequestId();
        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Kafka 메시지 전송 실패: requestId={}, quizId={}, memberId={}", requestId, quizId, memberId, ex);
                rewardQuotaService.incrementWinnerSlot(quizId);
            } else {
                log.debug("Kafka 메시지 전송 성공: requestId={}, quizId={}, memberId={}, offset={}",
                        requestId, quizId, memberId, result.getRecordMetadata().offset());
            }
        });

        return requestId;
    }

    public QuizSubmission getSubmissionByRequestId(String requestId) {
        return quizSubmissionRepository.findByRequestId(requestId)
                .orElseThrow(() -> new IllegalArgumentException("응모 이력을 찾을 수 없습니다: " + requestId));
    }
}
