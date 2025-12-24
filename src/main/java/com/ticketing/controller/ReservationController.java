package com.ticketing.controller;

import com.ticketing.domain.reservation.Reservation;
import com.ticketing.service.ReservationService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/reservations")
@RequiredArgsConstructor
public class ReservationController {

    private final ReservationService reservationService;

    @PostMapping
    public ResponseEntity<ReservationResponse> reserve(@RequestBody ReservationRequest request) {
        Reservation reservation = reservationService.reserve(request.concertId(), request.memberId());
        ReservationResponse response = new ReservationResponse(
                reservation.getId(),
                reservation.getConcert().getId(),
                reservation.getMember().getId(),
                reservation.getStatus().name()
        );
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
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
}

