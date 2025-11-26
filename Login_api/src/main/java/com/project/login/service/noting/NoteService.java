package com.project.login.service.noting;

import com.project.login.convert.NoteConvert;
import com.project.login.mapper.NoteMapper;
import com.project.login.mapper.NotebookMapper;
import com.project.login.mapper.TagMapper;
import com.project.login.model.dataobject.NoteDO;
import com.project.login.model.dataobject.NotebookDO;
import com.project.login.model.dto.note.*;
import com.project.login.model.entity.NoteEntity;
import com.project.login.model.entity.NotebookEntity;
import com.project.login.model.vo.NoteVO;
import com.project.login.service.minio.MinioService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j // 假设使用 Slf4j 进行日志记录
public class NoteService {

    private final NoteMapper noteMapper;
    private final NotebookMapper notebookMapper;
    private final TagMapper tagMapper;
    private final MinioService minioservice;

    @Qualifier("noteConvert")
    private final NoteConvert convert;


    @Transactional
    public NoteVO createNote(NoteCreateDTO dto) {

        if (notebookMapper.selectById(dto.getMeta().getNotebookId()) == null) {
            throw new RuntimeException("笔记本不存在");
        }

        // 上传文件并获取文件名和URL
        MultipartFile file = dto.getFile();
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

        return convert.toVO(note);
    }


    @Transactional
    public NoteVO updateNote(NoteUpdateDTO dto) {

        NoteDO existing = noteMapper.selectById(dto.getMeta().getId());
        if (existing == null) throw new RuntimeException("笔记不存在");

        // 上传文件并获取文件名和URL
        MultipartFile file = dto.getFile();
        String name = minioservice.uploadFile(file);
        String url = minioservice.getFileUrl(name);

        existing.setTitle(dto.getMeta().getTitle());
        existing.setFileType(dto.getMeta().getFileType());
        existing.setFilename(name);
        existing.setUpdatedAt(LocalDateTime.now());

        noteMapper.update(existing);

        return convert.toVO(existing);
    }

    @Transactional
    public void deleteNote(NoteDeleteDTO dto) {

        NoteDO existing = noteMapper.selectById(dto.getNoteId());
        if (existing == null) throw new RuntimeException("笔记不存在");

        minioservice.deleteFile(existing.getFilename());

        noteMapper.deleteById(dto.getNoteId());
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
        // 3. 转 VO
        return convert.toVO(note);
    }
}