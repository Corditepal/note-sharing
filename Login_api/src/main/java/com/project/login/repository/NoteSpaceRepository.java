package com.project.login.repository;

import com.project.login.model.entity.NoteSpaceEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface NoteSpaceRepository extends JpaRepository<NoteSpaceEntity, Long> {

    // 根据用户查找所有笔记空间
    List<NoteSpaceEntity> findEntitiesByUserId(Long userId);

    // 删除空间时可直接用 deleteById
    List<NoteSpaceEntity> deleteEntityById(Long noteId);
}
