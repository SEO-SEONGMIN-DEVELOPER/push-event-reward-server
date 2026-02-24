package com.pushquiz.controller;

import com.pushquiz.domain.member.Member;
import com.pushquiz.domain.member.MemberRepository;
import com.pushquiz.domain.quiz.TimeAttackQuiz;
import com.pushquiz.domain.quiz.TimeAttackQuizRepository;
import com.pushquiz.domain.reservation.QuizSubmissionRepository;
import com.pushquiz.dto.QuizSubmissionEvent;
import com.pushquiz.service.RewardQuotaService;
import com.pushquiz.service.RewardQuotaSyncService;
import jakarta.persistence.EntityManager;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/test")
@RequiredArgsConstructor
public class TestDataController {

    private final QuizSubmissionRepository quizSubmissionRepository;
    private final TimeAttackQuizRepository timeAttackQuizRepository;
    private final MemberRepository memberRepository;
    private final RewardQuotaService rewardQuotaService;
    private final RewardQuotaSyncService rewardQuotaSyncService;
    private final org.springframework.kafka.core.KafkaTemplate<String, Object> kafkaTemplate;
    private final EntityManager entityManager;

    @PostMapping("/init")
    @Transactional
    public ResponseEntity<InitResponse> initializeTestData() {
        entityManager.createNativeQuery("SET FOREIGN_KEY_CHECKS = 0").executeUpdate();
        entityManager.createNativeQuery("TRUNCATE TABLE reservations").executeUpdate();
        entityManager.createNativeQuery("TRUNCATE TABLE quizzes").executeUpdate();
        entityManager.createNativeQuery("TRUNCATE TABLE members").executeUpdate();
        entityManager.createNativeQuery("SET FOREIGN_KEY_CHECKS = 1").executeUpdate();

        List<TimeAttackQuiz> quizzes = new ArrayList<>();
        quizzes.add(new TimeAttackQuiz("12시 타임어택 퀴즈 1", 100000, 100000));
        quizzes.add(new TimeAttackQuiz("12시 타임어택 퀴즈 2", 100000, 100000));
        quizzes.add(new TimeAttackQuiz("12시 타임어택 퀴즈 3", 100000, 100000));
        quizzes.add(new TimeAttackQuiz("12시 타임어택 퀴즈 4", 100000, 100000));
        quizzes.add(new TimeAttackQuiz("12시 타임어택 퀴즈 5", 100000, 100000));
        quizzes.add(new TimeAttackQuiz("12시 타임어택 퀴즈 6", 100000, 100000));
        quizzes.add(new TimeAttackQuiz("12시 타임어택 퀴즈 7", 100000, 100000));
        quizzes.add(new TimeAttackQuiz("12시 타임어택 퀴즈 8", 100000, 100000));
        quizzes.add(new TimeAttackQuiz("12시 타임어택 퀴즈 9", 100000, 100000));
        quizzes.add(new TimeAttackQuiz("12시 타임어택 퀴즈 10", 100000, 100000));
        quizzes = timeAttackQuizRepository.saveAll(quizzes);

        for (TimeAttackQuiz quiz : quizzes) {
            rewardQuotaService.initializeRewardQuota(quiz.getId(), 100000);
        }

        List<Member> members = new ArrayList<>();
        for (int i = 1; i <= 200; i++) {
            members.add(new Member(String.format("회원%03d", i)));
        }
        members = memberRepository.saveAll(members);

        List<Long> quizIds = quizzes.stream()
                .map(TimeAttackQuiz::getId)
                .toList();

        List<Long> memberIds = members.stream()
                .map(Member::getId)
                .toList();

        return ResponseEntity.status(HttpStatus.CREATED)
                .body(new InitResponse(
                        quizzes.size(),
                        members.size(),
                        quizIds,
                        memberIds,
                        "타임어택 퀴즈 테스트 데이터가 성공적으로 초기화되었습니다."
                ));
    }

    @PostMapping("/sync/db-to-redis")
    public ResponseEntity<SyncResponse> syncDbToRedis() {
        rewardQuotaSyncService.syncDbToRedis();
        return ResponseEntity.ok(new SyncResponse("DB → Redis 리워드 수량 동기화가 완료되었습니다."));
    }

    @PostMapping("/sync/redis-to-db")
    public ResponseEntity<SyncResponse> syncRedisToDB() {
        rewardQuotaSyncService.syncRedisToDB();
        return ResponseEntity.ok(new SyncResponse("Redis → DB 리워드 수량 동기화가 완료되었습니다."));
    }

    @PostMapping("/sync/quiz/{quizId}")
    public ResponseEntity<SyncResponse> syncQuizRewardQuota(
            @PathVariable Long quizId,
            @RequestParam(defaultValue = "db") String source) {
        rewardQuotaSyncService.syncRewardQuotaForQuiz(quizId, source);
        return ResponseEntity.ok(new SyncResponse(
                String.format("퀴즈 %d의 리워드 수량이 %s 기준으로 동기화되었습니다.", quizId, source.toUpperCase())
        ));
    }

    @GetMapping("/sync/report")
    public ResponseEntity<SyncResponse> reportRewardQuotaMismatch() {
        rewardQuotaSyncService.reportRewardQuotaMismatch();
        return ResponseEntity.ok(new SyncResponse("리워드 수량 불일치 리포트가 로그에 출력되었습니다."));
    }

    @GetMapping("/reward-quota/{quizId}")
    public ResponseEntity<RewardQuotaStatusResponse> getRewardQuotaStatus(@PathVariable Long quizId) {
        TimeAttackQuiz quiz = timeAttackQuizRepository.findById(quizId)
                .orElseThrow(() -> new IllegalArgumentException("퀴즈를 찾을 수 없습니다: " + quizId));

        Integer redisSlots = rewardQuotaService.getRemainingWinnerSlots(quizId);
        Integer dbSlots = quiz.getRemainingWinnerSlots();
        boolean synced = redisSlots != null && redisSlots.equals(dbSlots);

        return ResponseEntity.ok(new RewardQuotaStatusResponse(
                quizId,
                quiz.getTitle(),
                redisSlots,
                dbSlots,
                synced,
                synced ? 0 : Math.abs((redisSlots != null ? redisSlots : 0) - dbSlots)
        ));
    }

    public record InitResponse(
            int quizCount,
            int memberCount,
            List<Long> quizIds,
            List<Long> memberIds,
            String message
    ) {
    }

    public record SyncResponse(String message) {
    }

    public record RewardQuotaStatusResponse(
            Long quizId,
            String title,
            Integer redisSlots,
            Integer dbSlots,
            boolean synced,
            int difference
    ) {
    }

    @PostMapping("/force-dlq")
    public ResponseEntity<Map<String, String>> forceDlqTest() {
        QuizSubmissionEvent event = new QuizSubmissionEvent(99999L, 99999L);
        kafkaTemplate.send("quiz_submission", event.getRequestId(), event);

        Map<String, String> response = new HashMap<>();
        response.put("message", "DLQ 테스트 이벤트 전송 완료");
        response.put("requestId", event.getRequestId());
        response.put("quizId", "99999");
        response.put("memberId", "99999");
        response.put("info", "약 7초 후 DLQ 로그를 확인하세요");

        return ResponseEntity.ok(response);
    }
}
