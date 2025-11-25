package com.project.login.controller;

import com.project.login.model.dto.notespace.NoteSpaceByUserDTO;
import com.project.login.model.request.notespace.*;
import com.project.login.model.dto.notespace.NoteSpaceCreateDTO;
import com.project.login.model.dto.notespace.NoteSpaceUpdateDTO;
import com.project.login.model.dto.notespace.NoteSpaceDeleteDTO;
import com.project.login.model.vo.NoteSpaceVO;
import com.project.login.model.response.StandardResponse;
import com.project.login.service.noting.NoteSpaceService;
import com.project.login.convert.NoteSpaceConvert;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springdoc.core.annotations.ParameterObject;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.*;

import jakarta.validation.Valid;
import java.util.List;

@Tag(name = "Note Spaces", description = "Manage note spaces")
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/noting/spaces")
public class NoteSpaceController {

    private final NoteSpaceService notespaceService;
    @Qualifier("noteSpaceConvert")
    private final NoteSpaceConvert convert;

    @Operation(summary = "Create note space")
    @PostMapping
    public StandardResponse<NoteSpaceVO> createSpace(@Valid @RequestBody NoteSpaceCreateRequest request) {
        NoteSpaceCreateDTO dto = convert.toCreateDTO(request);
        NoteSpaceVO vo = notespaceService.createSpace(dto);
        return StandardResponse.success(vo);
    }

    @Operation(summary = "Rename space")
    @PutMapping
    public StandardResponse<NoteSpaceVO> updateSpace(@Valid @RequestBody NoteSpaceUpdateRequest request) {
        NoteSpaceUpdateDTO dto = convert.toUpdateDTO(request);
        NoteSpaceVO vo = notespaceService.renameSpace(dto);
        return StandardResponse.success(vo);
    }

    @Operation(summary = "Delete space")
    @DeleteMapping
    public StandardResponse<Void> deleteSpace(@Valid @RequestBody NoteSpaceDeleteRequest request) {
        NoteSpaceDeleteDTO dto = convert.toDeleteDTO(request);
        notespaceService.deleteSpace(dto);
        return StandardResponse.success(null);
    }

    @Operation(summary = "Get all note spaces by user ID")
    @PostMapping("/user")
    public StandardResponse<List<NoteSpaceVO>> getSpacesByUserId(
            @RequestBody @Valid NoteSpaceListByUserRequest request) {
        NoteSpaceByUserDTO dto = convert.toByUserDTO(request);
        List<NoteSpaceVO> voList = notespaceService.getSpacesByUserId(dto);
        return StandardResponse.success(voList);
    }
}
