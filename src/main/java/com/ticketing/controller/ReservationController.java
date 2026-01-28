package com.ticketing.controller;

import com.ticketing.domain.reservation.Reservation;
import com.ticketing.facade.ConcertFacade;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

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

    @PostMapping("/async")
    public ResponseEntity<?> reserveAsync(@RequestBody ReservationRequest request) {
        try {
            String requestId = concertFacade.reserveWithKafka(request.concertId(), request.memberId());
            AsyncReservationResponse response = new AsyncReservationResponse(
                    requestId,
                    request.concertId(),
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
    public ResponseEntity<?> getReservationStatus(@PathVariable String requestId) {
        try {
            Reservation reservation = concertFacade.getReservationByRequestId(requestId);
            ReservationStatusResponse response = new ReservationStatusResponse(
                    reservation.getRequestId(),
                    reservation.getId(),
                    reservation.getConcert().getId(),
                    reservation.getMember().getId(),
                    reservation.getStatus().name()
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

    public record AsyncReservationResponse(
            String requestId,
            Long concertId,
            Long memberId,
            String status
    ) {
    }

    public record ReservationStatusResponse(
            String requestId,
            Long reservationId,
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

