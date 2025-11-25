package com.project.login.service.noting;

import com.project.login.convert.NotebookConvert;
import com.project.login.mapper.NotebookMapper; // 假设使用 mapper 包
import com.project.login.mapper.NoteMapper;
import com.project.login.mapper.NoteSpaceMapper;
import com.project.login.mapper.TagMapper;
import com.project.login.model.dataobject.NoteSpaceDO;
import com.project.login.model.dataobject.NotebookDO;
import com.project.login.model.dataobject.TagDO;
import com.project.login.model.dto.notebook.*;
import com.project.login.model.vo.NotebookVO;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class NotebookService {

    private final NotebookMapper notebookMapper;
    private final NoteMapper noteMapper;
    private final NoteSpaceMapper noteSpaceMapper;
    private final TagMapper tagMapper;

    @Qualifier("notebookConvert")
    private final NotebookConvert convert;

    // --- 创建笔记本 ---
    @Transactional
    public NotebookVO createNotebook(NotebookCreateDTO dto) {

        if (noteSpaceMapper.selectById(dto.getSpaceId()) == null) {
            throw new RuntimeException("笔记空间不存在");
        }

        Long tagId = tagMapper.selectIdByName(dto.getTag());
        if (tagId == null) {
            TagDO newTag = TagDO.builder()
                    .name(dto.getTag())
                    .build();
            tagMapper.insertTag(newTag);
            tagId = newTag.getId();
        }

        NotebookDO notebookdo = NotebookDO.builder()
                .name(dto.getName())
                .spaceId(dto.getSpaceId())
                .tagId(tagId)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();

        notebookMapper.insert(notebookdo);

        return convert.toVO(notebookdo);
    }

    // --- 笔记本重命名 / 更新标签 ---
    @Transactional
    public NotebookVO updateNotebook(NotebookUpdateDTO dto) {

        NotebookDO existing = notebookMapper.selectById(dto.getId());
        if (existing == null) throw new RuntimeException("笔记不存在");

        Long oldTagId = existing.getTagId();
        Long tagId = tagMapper.selectIdByName(dto.getTag());
        if (tagId == null) {
            TagDO newTag = TagDO.builder()
                    .name(dto.getTag())
                    .build();
            tagMapper.insertTag(newTag);
            tagId = newTag.getId();
        }

        existing.setName(dto.getName());
        existing.setTagId(tagId);
        existing.setUpdatedAt(LocalDateTime.now());

        // 4. 使用 Mapper 更新数据库
        notebookMapper.update(existing);

        Integer spaceCount = noteSpaceMapper.countByTagId(oldTagId);

        Integer notebookCount = notebookMapper.countByTagId(oldTagId);

        if (spaceCount == 0 && notebookCount == 0) {
            tagMapper.deleteById(oldTagId);
        }

        return convert.toVO(existing);
    }

    // --- 删除笔记本（级联删除笔记） ---
    @Transactional
    public void deleteNotebook(NotebookDeleteDTO dto) {

        NotebookDO notebook = notebookMapper.selectById(dto.getId());
        if (notebook == null) throw new RuntimeException("笔记空间不存在");

        Long tagId = notebook.getTagId();

        noteMapper.deleteByNotebookId(notebook.getId());

        notebookMapper.deleteById(notebook.getId());

        Integer spaceCount = noteSpaceMapper.countByTagId(tagId);

        Integer notebookCount = notebookMapper.countByTagId(tagId);

        if (spaceCount == 0 && notebookCount == 0) {
            tagMapper.deleteById(tagId);
        }
    }

    // --- 移动笔记本到另一个笔记空间 ---
    @Transactional
    public NotebookVO moveNotebook(NotebookMoveDTO dto) {

        NotebookDO notebook = notebookMapper.selectById(dto.getNotebookId());
        if (notebook == null) throw new RuntimeException("笔记不存在");

        NoteSpaceDO targetSpace = noteSpaceMapper.selectById(dto.getTargetNoteSpaceId());
        if (targetSpace == null) throw new RuntimeException("目标笔记空间不存在");

        notebook.setSpaceId(targetSpace.getId());
        notebook.setUpdatedAt(LocalDateTime.now());

        notebookMapper.update(notebook);

        return convert.toVO(notebook);
    }

    // --- 根据笔记空间获取所有笔记本 ---
    @Transactional(readOnly = true)
    public List<NotebookVO> getNotebooksBySpace(NotebookBySpaceDTO dto) {

        if (noteSpaceMapper.selectById(dto.getSpaceId()) == null) throw new RuntimeException("空间不存在");

        List<NotebookDO> doList = notebookMapper.selectBySpaceId(dto.getSpaceId());
        return doList.stream().map(convert::toVO).collect(Collectors.toList());
    }
}