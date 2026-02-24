package com.pushquiz.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

import java.time.LocalDateTime;

@Getter
public class DeadLetterEvent {

    private final QuizSubmissionEvent originalEvent;
    private final String errorMessage;
    private final String errorType;
    private final LocalDateTime failedAt;
    private final int retryAttempts;
    private final String partition;
    private final String offset;

    @JsonCreator
    public DeadLetterEvent(
            @JsonProperty("originalEvent") QuizSubmissionEvent originalEvent,
            @JsonProperty("errorMessage") String errorMessage,
            @JsonProperty("errorType") String errorType,
            @JsonProperty("failedAt") LocalDateTime failedAt,
            @JsonProperty("retryAttempts") int retryAttempts,
            @JsonProperty("partition") String partition,
            @JsonProperty("offset") String offset
    ) {
        this.originalEvent = originalEvent;
        this.errorMessage = errorMessage;
        this.errorType = errorType;
        this.failedAt = failedAt;
        this.retryAttempts = retryAttempts;
        this.partition = partition;
        this.offset = offset;
    }

    public static DeadLetterEvent of(
            QuizSubmissionEvent originalEvent,
            Exception exception,
            Integer partition,
            Long offset
    ) {
        return new DeadLetterEvent(
                originalEvent,
                exception.getMessage(),
                exception.getClass().getSimpleName(),
                LocalDateTime.now(),
                3,
                String.valueOf(partition),
                String.valueOf(offset)
        );
    }
}
