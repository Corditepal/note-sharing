package com.project.login.repository;

import com.project.login.model.entity.CommentEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.List;

public interface CommentRepository extends JpaRepository<CommentEntity, Long> {

    @Query("select c from CommentEntity c join c.user u join c.note n " +
            "where (:publisher is null or u.username like %:publisher%) " +
            "and (:start is null or c.createdAt >= :start) " +
            "and (:end is null or c.createdAt < :end)")
    List<CommentEntity> search(@Param("publisher") String publisher,
                               @Param("start") LocalDateTime start,
                               @Param("end") LocalDateTime end);
}