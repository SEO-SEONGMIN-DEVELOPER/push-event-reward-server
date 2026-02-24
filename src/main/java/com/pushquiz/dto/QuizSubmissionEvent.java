package com.pushquiz.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.util.UUID;

@Getter
@ToString
public class QuizSubmissionEvent implements Serializable {

    private final String requestId;
    private final Long quizId;
    private final Long memberId;
    private final Long timestamp;

    @JsonCreator
    public QuizSubmissionEvent(
            @JsonProperty("requestId") String requestId,
            @JsonProperty("quizId") Long quizId,
            @JsonProperty("memberId") Long memberId,
            @JsonProperty("timestamp") Long timestamp
    ) {
        this.requestId = requestId != null ? requestId : UUID.randomUUID().toString();
        this.quizId = quizId;
        this.memberId = memberId;
        this.timestamp = timestamp != null ? timestamp : System.currentTimeMillis();
    }

    public QuizSubmissionEvent(Long quizId, Long memberId) {
        this(UUID.randomUUID().toString(), quizId, memberId, System.currentTimeMillis());
    }
}
