package com.project.login.repository;

import com.project.login.model.entity.NoteEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

@Repository
public interface NoteRepository extends JpaRepository<NoteEntity, Long> {

    // 根据笔记本ID查询笔记
    List<NoteEntity> findByNotebookId(Long notebookId);

    // 根据笔记本ID删除笔记
    void deleteByNotebookId(Long notebookId);

    @Query("select distinct n from NoteEntity n " +
            "join fetch n.notebook nb " +
            "join fetch nb.space s " +
            "join fetch s.user u " +
            "join fetch nb.tag t " +
            "where (:author is null or lower(u.username) like lower(concat('%', :author, '%'))) " +
            "and (:tag is null or lower(t.name) like lower(concat('%', :tag, '%')))")
    List<NoteEntity> searchForAdmin(@Param("author") String author, @Param("tag") String tag);
}
