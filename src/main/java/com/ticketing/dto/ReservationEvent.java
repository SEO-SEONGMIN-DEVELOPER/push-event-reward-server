package com.ticketing.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

@Getter
@ToString
public class ReservationEvent implements Serializable {

    private final Long concertId;
    private final Long memberId;
    private final Long timestamp;

    @JsonCreator
    public ReservationEvent(
            @JsonProperty("concertId") Long concertId,
            @JsonProperty("memberId") Long memberId,
            @JsonProperty("timestamp") Long timestamp
    ) {
        this.concertId = concertId;
        this.memberId = memberId;
        this.timestamp = timestamp != null ? timestamp : System.currentTimeMillis();
    }

    public ReservationEvent(Long concertId, Long memberId) {
        this(concertId, memberId, System.currentTimeMillis());
    }
}

