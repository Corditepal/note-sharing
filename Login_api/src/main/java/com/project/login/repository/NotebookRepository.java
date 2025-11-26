package com.project.login.repository;

import com.project.login.model.entity.NotebookEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface NotebookRepository extends JpaRepository<NotebookEntity, Long> {

    // 根据空间ID查询所有笔记本
    List<NotebookEntity> findBySpaceId(Long spaceId);

    // 根据空间ID删除所有笔记本（级联删除笔记）
    void deleteBySpaceId(Long spaceId);
}
