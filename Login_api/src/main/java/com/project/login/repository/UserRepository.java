package com.project.login.repository;

import com.project.login.model.entity.UserEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;


import java.util.List;
import java.util.Optional;

public interface UserRepository extends JpaRepository<UserEntity, Long> {

    boolean existsByEmail(String email);

    boolean existsByStudentNumber(String studentNumber);

    Optional<UserEntity> findByEmail(String email);

    Optional<UserEntity> findByStudentNumber(String studentNumber);

    List<UserEntity> findByStudentNumberContaining(String studentNumber);

    @Query("select u from UserEntity u left join fetch u.roles where u.email = :email")
    Optional<UserEntity> findWithRolesByEmail(@Param("email") String email);
}
