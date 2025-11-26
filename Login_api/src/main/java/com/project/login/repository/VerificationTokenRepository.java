package com.project.login.repository;

import com.project.login.model.entity.VerificationTokenEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface VerificationTokenRepository extends JpaRepository<VerificationTokenEntity, Long> {

    Optional<VerificationTokenEntity> findTopByEmailAndTypeOrderByCreatedAtDesc(String email, String type);

}
