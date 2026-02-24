package com.pushquiz.service;

import com.pushquiz.domain.quiz.TimeAttackQuiz;
import com.pushquiz.domain.quiz.TimeAttackQuizRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class RewardQuotaSyncService {

    private final TimeAttackQuizRepository timeAttackQuizRepository;
    private final RewardQuotaService rewardQuotaService;

    @Scheduled(fixedRate = 300000)
    @Transactional(readOnly = true)
    public void syncDbToRedis() {
        log.info("DB → Redis 리워드 수량 동기화 시작");

        List<TimeAttackQuiz> quizzes = timeAttackQuizRepository.findAll();
        int syncCount = 0;

        for (TimeAttackQuiz quiz : quizzes) {
            Integer redisSlots = rewardQuotaService.getRemainingWinnerSlots(quiz.getId());
            Integer dbSlots = quiz.getRemainingWinnerSlots();

            if (redisSlots == null || !redisSlots.equals(dbSlots)) {
                rewardQuotaService.syncRewardQuota(quiz.getId(), dbSlots);
                log.warn("리워드 수량 불일치 감지 및 동기화: quizId={}, Redis={}, DB={} → DB 기준 동기화",
                        quiz.getId(), redisSlots, dbSlots);
                syncCount++;
            }
        }

        log.info("DB → Redis 리워드 수량 동기화 완료: 총 {}개 퀴즈, {}개 동기화", quizzes.size(), syncCount);
    }

    @Transactional
    public void syncRedisToDB() {
        log.info("Redis → DB 리워드 수량 동기화 시작");

        List<TimeAttackQuiz> quizzes = timeAttackQuizRepository.findAll();
        int syncCount = 0;

        for (TimeAttackQuiz quiz : quizzes) {
            Integer redisSlots = rewardQuotaService.getRemainingWinnerSlots(quiz.getId());

            if (redisSlots != null && !redisSlots.equals(quiz.getRemainingWinnerSlots())) {
                timeAttackQuizRepository.updateRemainingWinnerSlots(quiz.getId(), redisSlots);
                log.info("리워드 수량 동기화: quizId={}, DB={} → Redis={}",
                        quiz.getId(), quiz.getRemainingWinnerSlots(), redisSlots);
                syncCount++;
            }
        }

        log.info("Redis → DB 리워드 수량 동기화 완료: 총 {}개 퀴즈, {}개 동기화", quizzes.size(), syncCount);
    }

    @Transactional
    public void syncRewardQuotaForQuiz(Long quizId, String source) {
        TimeAttackQuiz quiz = timeAttackQuizRepository.findById(quizId)
                .orElseThrow(() -> new IllegalArgumentException("퀴즈를 찾을 수 없습니다: " + quizId));

        Integer redisSlots = rewardQuotaService.getRemainingWinnerSlots(quizId);
        Integer dbSlots = quiz.getRemainingWinnerSlots();

        if ("redis".equalsIgnoreCase(source)) {
            if (redisSlots != null) {
                timeAttackQuizRepository.updateRemainingWinnerSlots(quizId, redisSlots);
                log.info("Redis 기준 동기화: quizId={}, DB={} → Redis={}",
                        quizId, dbSlots, redisSlots);
            }
        } else {
            rewardQuotaService.syncRewardQuota(quizId, dbSlots);
            log.info("DB 기준 동기화: quizId={}, Redis={} → DB={}",
                    quizId, redisSlots, dbSlots);
        }
    }

    @Transactional(readOnly = true)
    public void reportRewardQuotaMismatch() {
        log.info("리워드 수량 불일치 감지 시작");

        List<TimeAttackQuiz> quizzes = timeAttackQuizRepository.findAll();
        int mismatchCount = 0;

        for (TimeAttackQuiz quiz : quizzes) {
            Integer redisSlots = rewardQuotaService.getRemainingWinnerSlots(quiz.getId());
            Integer dbSlots = quiz.getRemainingWinnerSlots();

            if (redisSlots != null && !redisSlots.equals(dbSlots)) {
                log.warn("리워드 수량 불일치: quizId={}, title={}, Redis={}, DB={}, 차이={}",
                        quiz.getId(), quiz.getTitle(), redisSlots, dbSlots,
                        Math.abs(redisSlots - dbSlots));
                mismatchCount++;
            }
        }

        if (mismatchCount == 0) {
            log.info("리워드 수량 불일치 없음: 모든 퀴즈({})의 Redis-DB 수량 일치", quizzes.size());
        } else {
            log.warn("리워드 수량 불일치 감지: 총 {}개 퀴즈 중 {}개 불일치", quizzes.size(), mismatchCount);
        }
    }
}
