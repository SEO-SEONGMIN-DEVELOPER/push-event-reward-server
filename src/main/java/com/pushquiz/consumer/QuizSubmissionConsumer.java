package com.pushquiz.consumer;

import com.pushquiz.domain.member.Member;
import com.pushquiz.domain.member.MemberRepository;
import com.pushquiz.domain.quiz.TimeAttackQuiz;
import com.pushquiz.domain.quiz.TimeAttackQuizRepository;
import com.pushquiz.domain.reservation.QuizSubmission;
import com.pushquiz.domain.reservation.QuizSubmissionRepository;
import com.pushquiz.domain.reservation.QuizSubmissionStatus;
import com.pushquiz.dto.DeadLetterEvent;
import com.pushquiz.dto.QuizSubmissionEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class QuizSubmissionConsumer {

    private static final String DLQ_TOPIC = "quiz_submission_dlq";

    private final QuizSubmissionRepository quizSubmissionRepository;
    private final TimeAttackQuizRepository timeAttackQuizRepository;
    private final MemberRepository memberRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(
            topics = "quiz_submission",
            groupId = "push-quiz-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional
    public void consumeQuizSubmissionEvents(
            @Payload List<QuizSubmissionEvent> events,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) List<Integer> partitions,
            @Header(KafkaHeaders.OFFSET) List<Long> offsets,
            Acknowledgment acknowledgment
    ) {
        log.info("퀴즈 응모 이벤트 수신: topic={}, batchSize={}", topic, events.size());

        List<QuizSubmission> submissions = new ArrayList<>();
        List<TimeAttackQuiz> quizzesToUpdate = new ArrayList<>();
        int successCount = 0;
        int failureCount = 0;

        for (int i = 0; i < events.size(); i++) {
            QuizSubmissionEvent event = events.get(i);
            Integer partition = partitions.get(i);
            Long offset = offsets.get(i);

            try {
                QuizSubmission submission = processQuizSubmissionWithRetry(event, partition, offset);
                submission.complete();
                submissions.add(submission);

                TimeAttackQuiz quiz = submission.getQuiz();
                quiz.claimWinnerSlot();
                quizzesToUpdate.add(quiz);

                successCount++;
                log.debug("퀴즈 응모 생성 성공: requestId={}, quizId={}, memberId={}, partition={}, offset={}",
                        event.getRequestId(), event.getQuizId(), event.getMemberId(), partition, offset);
            } catch (Exception e) {
                failureCount++;
                log.error("퀴즈 응모 처리 최종 실패 (3회 재시도 후): requestId={}, quizId={}, memberId={}, partition={}, offset={}, error={}",
                        event.getRequestId(), event.getQuizId(), event.getMemberId(), partition, offset, e.getMessage(), e);

                sendToDeadLetterQueue(event, e, partition, offset);

                try {
                    Member member = memberRepository.findById(event.getMemberId()).orElse(null);
                    TimeAttackQuiz quiz = timeAttackQuizRepository.findById(event.getQuizId()).orElse(null);

                    if (member != null && quiz != null) {
                        QuizSubmission failedSubmission = new QuizSubmission(
                                event.getRequestId(),
                                member,
                                quiz,
                                QuizSubmissionStatus.FAILED
                        );
                        submissions.add(failedSubmission);
                    }
                } catch (Exception ex) {
                    log.error("실패 응모 저장 중 오류: requestId={}, error={}", event.getRequestId(), ex.getMessage());
                }
            }
        }

        if (!submissions.isEmpty()) {
            quizSubmissionRepository.saveAll(submissions);
            log.debug("퀴즈 응모 DB 배치 저장 완료: count={}", submissions.size());
        }

        if (!quizzesToUpdate.isEmpty()) {
            timeAttackQuizRepository.saveAll(quizzesToUpdate);
            log.debug("퀴즈 리워드 수량 DB 동기화 완료: count={}", quizzesToUpdate.size());
        }

        log.info("퀴즈 응모 이벤트 처리 완료: 성공={}, 실패={}, 총={}", successCount, failureCount, events.size());

        if (acknowledgment != null) {
            acknowledgment.acknowledge();
        }
    }

    @Retryable(
            retryFor = Exception.class,
            maxAttempts = 3,
            backoff = @Backoff(delay = 1000, multiplier = 2)
    )
    public QuizSubmission processQuizSubmissionWithRetry(
            QuizSubmissionEvent event,
            Integer partition,
            Long offset
    ) {
        log.debug("퀴즈 응모 처리 시도: requestId={}, quizId={}, memberId={}, partition={}, offset={}",
                event.getRequestId(), event.getQuizId(), event.getMemberId(), partition, offset);

        TimeAttackQuiz quiz = timeAttackQuizRepository.findById(event.getQuizId())
                .orElseThrow(() -> new IllegalArgumentException(
                        String.format("퀴즈를 찾을 수 없습니다: requestId=%s, quizId=%d, partition=%d, offset=%d",
                                event.getRequestId(), event.getQuizId(), partition, offset)));

        Member member = memberRepository.findById(event.getMemberId())
                .orElseThrow(() -> new IllegalArgumentException(
                        String.format("회원을 찾을 수 없습니다: requestId=%s, memberId=%d, partition=%d, offset=%d",
                                event.getRequestId(), event.getMemberId(), partition, offset)));

        return new QuizSubmission(event.getRequestId(), member, quiz, QuizSubmissionStatus.PENDING);
    }

    private void sendToDeadLetterQueue(QuizSubmissionEvent event, Exception exception, Integer partition, Long offset) {
        try {
            DeadLetterEvent dlqEvent = DeadLetterEvent.of(event, exception, partition, offset);
            kafkaTemplate.send(DLQ_TOPIC, event.getRequestId(), dlqEvent);
            log.info("DLQ 전송 완료: requestId={}, topic={}, partition={}, offset={}",
                    event.getRequestId(), DLQ_TOPIC, partition, offset);
        } catch (Exception e) {
            log.error("DLQ 전송 실패: requestId={}, error={}", event.getRequestId(), e.getMessage(), e);
        }
    }
}
