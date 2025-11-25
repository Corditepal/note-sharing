package com.project.login.repository;

import com.project.login.model.entity.NoteEntity;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

@Repository
public interface NoteRepository extends ElasticsearchRepository<NoteEntity, Long> {

    // 根据完整标题查找
    Optional<NoteEntity> findByTitle(String title);

    // 根据id查找
    @NotNull
    Optional<NoteEntity> findById(@NotNull Long id);

    // 模糊搜索标题（like %title%）
    List<NoteEntity> findByTitleContaining(String title);

    // 根据作者查找笔记
    List<NoteEntity> findByAuthorName(String authorName);

    // 根据标签搜索（匹配任意一个标签）
    List<NoteEntity> findByTagsIn(Collection<List<String>> tags);
}
