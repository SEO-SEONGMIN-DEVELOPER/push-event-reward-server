package com.ticketing.controller;

import com.ticketing.domain.concert.Concert;
import com.ticketing.domain.concert.ConcertRepository;
import com.ticketing.domain.member.Member;
import com.ticketing.domain.member.MemberRepository;
import com.ticketing.domain.reservation.ReservationRepository;
import com.ticketing.service.InventoryService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/api/test")
@RequiredArgsConstructor
public class TestDataController {

    private final ReservationRepository reservationRepository;
    private final ConcertRepository concertRepository;
    private final MemberRepository memberRepository;
    private final InventoryService inventoryService;

    @PostMapping("/init")
    @Transactional
    public ResponseEntity<InitResponse> initializeTestData() {
        reservationRepository.deleteAll();
        concertRepository.deleteAll();
        memberRepository.deleteAll();

        List<Concert> concerts = new ArrayList<>();
        concerts.add(new Concert("2024 봄 콘서트", 10000, 10000));
        concerts.add(new Concert("2024 여름 페스티벌", 10000, 10000));
        concerts.add(new Concert("2024 가을 뮤지컬", 10000, 10000));
        concerts.add(new Concert("2024 겨울 오케스트라", 10000, 10000));
        concerts.add(new Concert("2024 연말 갈라쇼", 10000, 10000));
        concerts = concertRepository.saveAll(concerts);

        for (Concert concert : concerts) {
            inventoryService.initializeInventory(concert.getId(), 100000);
        }

        List<Member> members = new ArrayList<>();
        for (int i = 1; i <= 200; i++) {
            members.add(new Member(String.format("회원%03d", i)));
        }
        members = memberRepository.saveAll(members);

        List<Long> concertIds = concerts.stream()
                .map(Concert::getId)
                .toList();
        
        List<Long> memberIds = members.stream()
                .map(Member::getId)
                .toList();

        return ResponseEntity.status(HttpStatus.CREATED)
                .body(new InitResponse(
                        concerts.size(),
                        members.size(),
                        concertIds,
                        memberIds,
                        "테스트 데이터가 성공적으로 초기화되었습니다."
                ));
    }

    public record InitResponse(
            int concertCount,
            int memberCount,
            List<Long> concertIds,
            List<Long> memberIds,
            String message
    ) {
    }
}

