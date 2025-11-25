package com.project.login.service.noting;

import com.project.login.convert.NoteConvert;
import com.project.login.mapper.*;
import com.project.login.model.dataobject.*;
import com.project.login.model.dto.note.*;
import com.project.login.model.event.EsNoteEvent;
import com.project.login.model.event.NoteActionType;
import com.project.login.model.vo.NoteVO;
import com.project.login.service.minio.MinioService;
import com.project.login.service.tag.TagService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;

import java.time.LocalDateTime;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j // 假设使用 Slf4j 进行日志记录
public class NoteService {

    private final NoteMapper noteMapper;
    private final NotebookMapper notebookMapper;
    private final NoteSpaceMapper notespaceMapper;
    private final UserMapper userMapper;
    private final MinioService minioservice;
    private final TagService tagService;
    private final NoteEventPublisher eventPublisher;
    private final ContentSummaryService contentSummaryService;

    @Qualifier("noteConvert")
    private final NoteConvert convert;
    private final TagMapper tagMapper;


    @Transactional
    public NoteVO createNote(NoteCreateDTO dto) {

        if (notebookMapper.selectById(dto.getMeta().getNotebookId()) == null) {
            throw new RuntimeException("笔记本不存在");
        }

        // 上传文件并获取文件名和URL
        MultipartFile file = dto.getFile();
        String contentSummary = contentSummaryService.extractContentSummary(file);

        //minio 名字
        String name = minioservice.uploadFile(file);
        String url = minioservice.getFileUrl(name);

        NoteDO note = NoteDO.builder()
                .title(dto.getMeta().getTitle())
                .fileType(dto.getMeta().getFileType())
                .filename(name)
                .notebookId(dto.getMeta().getNotebookId())
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();

        noteMapper.insert(note);

        // --- 发布异步更新 ES 事件 ---
        EsNoteEvent event = new EsNoteEvent();
        event.setNoteId(note.getId());
        event.setAction(NoteActionType.CREATE);

        event.setTitle(note.getTitle());
        event.setContentSummary(contentSummary);

        NotebookDO notebook = notebookMapper.selectById(dto.getMeta().getNotebookId());
        NoteSpaceDO notespace = notespaceMapper.selectById(notebook.getSpaceId());
        UserDO user = userMapper.selectById(notespace.getUserId());
        event.setAuthorName(user.getUsername());

        event.setUpdatedAt(LocalDateTime.now());

        eventPublisher.sendEsNoteEvent(event);

        return convert.toVO(note);
    }


    @Transactional
    public NoteVO updateNote(NoteUpdateDTO dto) {

        NoteDO existing = noteMapper.selectById(dto.getMeta().getId());
        if (existing == null) throw new RuntimeException("笔记不存在");

        // 上传文件并获取文件名和URL
        MultipartFile file = dto.getFile();
        String contentSummary = contentSummaryService.extractContentSummary(file);
        String name = minioservice.uploadFile(file);

        existing.setTitle(dto.getMeta().getTitle());
        existing.setFileType(dto.getMeta().getFileType());
        existing.setFilename(name);
        existing.setUpdatedAt(LocalDateTime.now());

        noteMapper.update(existing);

        // --- 发布异步更新 ES 事件 ---
        EsNoteEvent event = new EsNoteEvent();
        event.setNoteId(existing.getId());
        event.setAction(NoteActionType.UPDATE);

        event.setTitle(existing.getTitle());
        event.setContentSummary(contentSummary);

        event.setUpdatedAt(LocalDateTime.now());

        eventPublisher.sendEsNoteEvent(event);

        return convert.toVO(existing);
    }

    @Transactional
    public void deleteNote(NoteDeleteDTO dto) {

        NoteDO existing = noteMapper.selectById(dto.getNoteId());
        if (existing == null) throw new RuntimeException("笔记不存在");

        minioservice.deleteFile(existing.getFilename());

        noteMapper.deleteById(dto.getNoteId());

        // --- 发布异步更新 ES 事件 ---
        EsNoteEvent event = new EsNoteEvent();
        event.setNoteId(existing.getId());
        event.setAction(NoteActionType.DELETE);

        eventPublisher.sendEsNoteEvent(event);
    }


    @Transactional
    public NoteVO moveNote(NoteMoveDTO dto) {

        NoteDO note = noteMapper.selectById(dto.getNoteId());
        if (note == null) throw new RuntimeException("笔记不存在");

        if (notebookMapper.selectById(dto.getTargetNotebookId()) == null) {
            throw new RuntimeException("笔记本不存在");
        }

        note.setNotebookId(dto.getTargetNotebookId());
        note.setUpdatedAt(LocalDateTime.now());

        noteMapper.update(note);

        return convert.toVO(note);
    }


    @Transactional(readOnly = true)
    public List<NoteVO> getNotesByNotebook(NoteListByNotebookDTO dto) {


        if (notebookMapper.selectById(dto.getNotebookID()) == null) {
            throw new RuntimeException("笔记本不存在");
        }
        List<NoteDO> doList = noteMapper.selectVOListByNotebookId(dto.getNotebookID());
        return convert.toVOList(doList);
    }

    // --- 文件/图片操作 ---
    @Transactional
    public NoteVO uploadFile(NoteUploadFileDTO dto) {

        if (notebookMapper.selectById(dto.getMeta().getNotebookId()) == null) {
            throw new RuntimeException("笔记本不存在");
        }

        // 上传文件并获取文件名和URL
        MultipartFile file = dto.getFile();
        String contentSummary = contentSummaryService.extractContentSummary(file);
        String name = minioservice.uploadFile(file);
        String url = minioservice.getFileUrl(name);

        NoteDO note = NoteDO.builder()
                .title(dto.getMeta().getTitle())
                .fileType(dto.getMeta().getFileType())
                .filename(name)
                .notebookId(dto.getMeta().getNotebookId())
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();

        noteMapper.insert(note);

        // --- 发布异步更新 ES 事件 ---
        EsNoteEvent event = new EsNoteEvent();
        event.setNoteId(note.getId());
        event.setAction(NoteActionType.CREATE);

        event.setTitle(note.getTitle());
        event.setContentSummary(contentSummary);

        NotebookDO notebook = notebookMapper.selectById(dto.getMeta().getNotebookId());
        NoteSpaceDO notespace = notespaceMapper.selectById(notebook.getSpaceId());

        UserDO user = userMapper.selectById(notespace.getUserId());
        event.setAuthorName(user.getUsername());

        event.setUpdatedAt(LocalDateTime.now());

        eventPublisher.sendEsNoteEvent(event);

        return convert.toVO(note);
    }


    @Transactional(readOnly = true)
    public String getFileUrl(NoteFileUrlDTO dto) {
        String url = minioservice.getFileUrl(dto.getFilename());
        return url;
    }


    @Transactional
    public String uploadImage(ImageUrlDTO dto) {
        // 上传文件并获取文件名和URL
        MultipartFile file = dto.getFile();
        String name = minioservice.uploadFile(file);
        String url = minioservice.getFileUrl(name);
        return url;
    }

    @Transactional
    public NoteVO renameNote(NoteRenameDTO dto) {
        // 1. 查找
        NoteDO note = noteMapper.selectById(dto.getId());
        if (note == null) throw new RuntimeException("笔记不存在");
        // 2. 更新名称
        note.setTitle(dto.getNewName());
        note.setUpdatedAt(LocalDateTime.now());
        noteMapper.update(note);

        EsNoteEvent event = new EsNoteEvent();
        event.setNoteId(note.getId());
        event.setAction(NoteActionType.UPDATE);

        event.setTitle(note.getTitle());

        eventPublisher.sendEsNoteEvent(event);

        return convert.toVO(note);
    }
}