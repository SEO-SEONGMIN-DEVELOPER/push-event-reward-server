package com.pushquiz.domain.reservation;

import com.pushquiz.domain.member.Member;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface QuizSubmissionRepository extends JpaRepository<QuizSubmission, Long> {
    List<QuizSubmission> findByMember(Member member);

    List<QuizSubmission> findByStatus(QuizSubmissionStatus status);

    Optional<QuizSubmission> findByRequestId(String requestId);
}
