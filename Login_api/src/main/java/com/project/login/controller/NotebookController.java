package com.project.login.controller;

import com.project.login.convert.NotebookConvert;
import com.project.login.model.dto.notebook.*;
import com.project.login.model.request.notebook.*;
import com.project.login.model.response.StandardResponse;
import com.project.login.model.vo.NotebookVO;
import com.project.login.service.noting.NotebookService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Tag(name = "Notebooks", description = "Manage notebooks")
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/noting/notebooks")
public class NotebookController {

    private final NotebookService notebookService;

    @Qualifier("notebookConvert")
    private final NotebookConvert convert;

    @Operation(summary = "Create notebook")
    @PostMapping
    public StandardResponse<NotebookVO> createNotebook(@Valid @RequestBody NotebookCreateRequest request) {
        NotebookCreateDTO dto = convert.toCreateDTO(request);
        NotebookVO vo = notebookService.createNotebook(dto);
        return StandardResponse.success(vo);
    }

    @Operation(summary = "Update notebook")
    @PutMapping
    public StandardResponse<NotebookVO> updateNotebook(@Valid @RequestBody NotebookUpdateRequest request) {
        NotebookUpdateDTO dto = convert.toUpdateDTO(request);
        NotebookVO vo = notebookService.updateNotebook(dto);
        return StandardResponse.success(vo);
    }

    @Operation(summary = "Delete notebook")
    @DeleteMapping
    public StandardResponse<Void> deleteNotebook(@Valid @RequestBody NotebookDeleteRequest request) {
        NotebookDeleteDTO dto = convert.toDeleteDTO(request);
        notebookService.deleteNotebook(dto);
        return StandardResponse.success(null);
    }

    @Operation(summary = "Move notebook")
    @PutMapping("/move-notebook")
    public StandardResponse<NotebookVO> moveNotebook(@Valid @RequestBody NotebookMoveRequest request) {
        NotebookMoveDTO dto = convert.toMoveDTO(request);
        NotebookVO vo = notebookService.moveNotebook(dto);
        return StandardResponse.success(vo);
    }

    @Operation(summary = "Get notebooks by note space")
    @PostMapping("/by-space")
    public StandardResponse<List<NotebookVO>> getNotebooksBySpace(
            @Valid @RequestBody NotebookListBySpaceRequest request) {
        NotebookBySpaceDTO dto = convert.toBySpaceDTO(request);
        List<NotebookVO> voList = notebookService.getNotebooksBySpace(dto);
        return StandardResponse.success(voList);
    }
}