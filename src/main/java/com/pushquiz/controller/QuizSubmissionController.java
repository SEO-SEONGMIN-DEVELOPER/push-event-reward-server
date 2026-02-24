package com.pushquiz.controller;

import com.pushquiz.domain.reservation.QuizSubmission;
import com.pushquiz.facade.PushQuizFacade;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/quiz-submissions")
@RequiredArgsConstructor
public class QuizSubmissionController {

    private final PushQuizFacade pushQuizFacade;

    @PostMapping
    public ResponseEntity<?> submit(@RequestBody QuizSubmissionRequest request) {
        try {
            QuizSubmission submission = pushQuizFacade.submit(request.quizId(), request.memberId());
            QuizSubmissionResponse response = new QuizSubmissionResponse(
                    submission.getId(),
                    submission.getQuiz().getId(),
                    submission.getMember().getId(),
                    submission.getStatus().name()
            );
            return ResponseEntity.status(HttpStatus.CREATED).body(response);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(new ErrorResponse("BAD_REQUEST", e.getMessage()));
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(new ErrorResponse("CONFLICT", e.getMessage()));
        } catch (Exception e) {
            String exceptionMessage = e.getMessage();
            String exceptionClass = e.getClass().getSimpleName();
            if (exceptionMessage != null && (
                    exceptionMessage.contains("Connection is not available") ||
                            exceptionMessage.contains("Timeout") ||
                            exceptionMessage.contains("Pool") ||
                            exceptionMessage.contains("HikariPool") ||
                            exceptionMessage.contains("connection timeout") ||
                            exceptionClass.contains("Hikari"))) {
                return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                        .body(new ErrorResponse("SERVICE_UNAVAILABLE", "데이터베이스 커넥션 풀이 부족합니다. 잠시 후 다시 시도해주세요."));
            }

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new ErrorResponse("INTERNAL_SERVER_ERROR", e.getMessage()));
        }
    }

    @PostMapping("/async")
    public ResponseEntity<?> submitAsync(@RequestBody QuizSubmissionRequest request) {
        try {
            String requestId = pushQuizFacade.submitWithKafka(request.quizId(), request.memberId());
            AsyncQuizSubmissionResponse response = new AsyncQuizSubmissionResponse(
                    requestId,
                    request.quizId(),
                    request.memberId(),
                    "PENDING"
            );
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(new ErrorResponse("BAD_REQUEST", e.getMessage()));
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(new ErrorResponse("CONFLICT", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new ErrorResponse("INTERNAL_SERVER_ERROR", e.getMessage()));
        }
    }

    @GetMapping("/request/{requestId}")
    public ResponseEntity<?> getSubmissionStatus(@PathVariable String requestId) {
        try {
            QuizSubmission submission = pushQuizFacade.getSubmissionByRequestId(requestId);
            QuizSubmissionStatusResponse response = new QuizSubmissionStatusResponse(
                    submission.getRequestId(),
                    submission.getId(),
                    submission.getQuiz().getId(),
                    submission.getMember().getId(),
                    submission.getStatus().name()
            );
            return ResponseEntity.ok(response);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(new ErrorResponse("NOT_FOUND", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new ErrorResponse("INTERNAL_SERVER_ERROR", e.getMessage()));
        }
    }

    public record QuizSubmissionRequest(
            Long quizId,
            Long memberId
    ) {
    }

    public record QuizSubmissionResponse(
            Long id,
            Long quizId,
            Long memberId,
            String status
    ) {
    }

    public record AsyncQuizSubmissionResponse(
            String requestId,
            Long quizId,
            Long memberId,
            String status
    ) {
    }

    public record QuizSubmissionStatusResponse(
            String requestId,
            Long submissionId,
            Long quizId,
            Long memberId,
            String status
    ) {
    }

    public record ErrorResponse(
            String error,
            String message
    ) {
    }
}
