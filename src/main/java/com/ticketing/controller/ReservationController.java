package com.ticketing.controller;

import com.ticketing.domain.reservation.Reservation;
import com.ticketing.facade.ConcertFacade;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.async.AsyncRequestTimeoutException;

import java.sql.SQLTimeoutException;
import java.sql.SQLTransientException;
import java.util.concurrent.TimeoutException;

@RestController
@RequestMapping("/api/reservations")
@RequiredArgsConstructor
public class ReservationController {

    private final ConcertFacade concertFacade;

    @PostMapping
    public ResponseEntity<?> reserve(@RequestBody ReservationRequest request) {
        try {
            Reservation reservation = concertFacade.reserve(request.concertId(), request.memberId());
            ReservationResponse response = new ReservationResponse(
                    reservation.getId(),
                    reservation.getConcert().getId(),
                    reservation.getMember().getId(),
                    reservation.getStatus().name()
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

    public record ReservationRequest(
            Long concertId,
            Long memberId
    ) {
    }

    public record ReservationResponse(
            Long id,
            Long concertId,
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

