package com.project.login.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.login.convert.NoteConvert; // 引入 Convert
import com.project.login.model.dto.note.*; // 引入 DTOs
import com.project.login.model.request.note.*;
import com.project.login.model.response.StandardResponse;
import com.project.login.model.vo.NoteVO; // 引入 VO
import com.project.login.service.noting.NoteService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import jakarta.validation.Valid;
import org.springframework.web.multipart.MultipartFile;
import java.util.List;

@Tag(name = "Notes", description = "Manage notes")
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/noting/notes")
public class NoteController {

    private final NoteService noteService;

    @Qualifier("noteConvert")
    private final NoteConvert convert; // 注入转换器

    // --- 笔记 CRUD 操作 ---

    @PostMapping(value = "/create", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    @Operation(summary = "Create note with file and meta")
    public StandardResponse<NoteVO> createNote(
            @Parameter(description = "Note metadata JSON string", required = true)
            @RequestPart("meta") String metaJson,
            @Parameter(description = "File to upload", required = true)
            @RequestPart("file") MultipartFile file
    ) throws Exception {
        // 1. 组装请求对象
        ObjectMapper mapper = new ObjectMapper();
        NoteMeta meta = mapper.readValue(metaJson, NoteMeta.class);
        NoteCreateRequest request = new NoteCreateRequest();
        request.setMeta(meta);
        request.setFile(file);

        // 2. 转换为 DTO
        NoteCreateDTO dto = convert.toCreateDTO(request);

        // 3. 调用 Service
        NoteVO vo = noteService.createNote(dto);
        return StandardResponse.success(vo);
    }

    @Operation(summary = "Update note")
    @PutMapping(value = "/update", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public StandardResponse<NoteVO> updateNote(
            @RequestPart("meta") String metaJson,
            @RequestPart(value = "file", required = false) MultipartFile file
    ) throws Exception {
        // 1. 组装请求对象
        ObjectMapper mapper = new ObjectMapper();
        NoteUpdateMeta meta = mapper.readValue(metaJson, NoteUpdateMeta.class);
        NoteUpdateRequest request = new NoteUpdateRequest();
        request.setMeta(meta);
        request.setFile(file);

        // 2. 转换为 DTO
        NoteUpdateDTO dto = convert.toUpdateDTO(request);

        // 3. 调用 Service
        NoteVO vo = noteService.updateNote(dto);
        return StandardResponse.success(vo);
    }

    @Operation(summary = "Delete note")
    @DeleteMapping
    public StandardResponse<Void> deleteNote(@Valid @RequestBody NoteDeleteRequest request) {
        NoteDeleteDTO dto = convert.toDeleteDTO(request);
        noteService.deleteNote(dto);
        return StandardResponse.success(null); // 参照 NoteSpaceController 返回 null
    }

    @Operation(summary = "Move note to another notebook")
    @PostMapping("/move")
    public StandardResponse<NoteVO> moveNote(@Valid @RequestBody NoteMoveRequest request) {
        NoteMoveDTO dto = convert.toMoveDTO(request);
        NoteVO vo = noteService.moveNote(dto);
        return StandardResponse.success(vo);
    }

    @Operation(summary = "Get notes by notebook")
    @PostMapping("/by-notebook")
    public StandardResponse<List<NoteVO>> getNotesByNotebook(@Valid @RequestBody NoteListByNotebookRequest request) {
        NoteListByNotebookDTO dto = convert.toListByNotebookDTO(request);
        List<NoteVO> voList = noteService.getNotesByNotebook(dto);
        return StandardResponse.success(voList);
    }

    // --- 文件/图片操作 ---

    @Operation(summary = "Upload attachment")
    @PostMapping(value = "/files", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public StandardResponse<NoteVO> uploadFile( // 假设返回文件URL字符串
                                                @RequestPart("meta") String metaJson,
                                                @RequestPart(value = "file", required = false) MultipartFile file
    ) throws Exception {
        // 1. 组装请求对象
        ObjectMapper mapper = new ObjectMapper();
        NoteMeta meta = mapper.readValue(metaJson, NoteMeta.class);
        NoteUploadFileRequest request = new NoteUploadFileRequest();
        request.setMeta(meta);
        request.setFile(file);

        // 2. 转换为 DTO
        NoteUploadFileDTO dto = convert.toUploadFileDTO(request);

        // 3. 调用 Service
        NoteVO vo = noteService.uploadFile(dto);
        return StandardResponse.success(vo);
    }

    @Operation(summary = "Get file access URL")
    @PostMapping("/files/url")
    public StandardResponse<String> getFileUrl(@Valid @RequestBody NoteFileUrlRequest request) {
        NoteFileUrlDTO dto = convert.toFileUrlDTO(request);
        String fileUrl = noteService.getFileUrl(dto);
        return StandardResponse.success(fileUrl);
    }

    @Operation(summary = "Upload image and return access URL")
    @PostMapping(value = "/image", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public StandardResponse<String> uploadImage(@RequestPart("file") MultipartFile file) {
        ImageUrlRequest request = new ImageUrlRequest();
        request.setFile(file);
        ImageUrlDTO dto = convert.toImageUrlDTO(request);
        String imageUrl = noteService.uploadImage(dto);
        return StandardResponse.success(imageUrl);
    }

    @Operation(summary = "Rename note")
    @PostMapping("/rename")
    public StandardResponse<NoteVO> renameNote(@Valid @RequestBody NoteRenameRequest request) {
        NoteRenameDTO dto = convert.toRenameDTO(request);
        NoteVO vo = noteService.renameNote(dto);
        return StandardResponse.success(vo);
    }

}