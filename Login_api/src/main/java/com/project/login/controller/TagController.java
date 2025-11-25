package com.project.login.controller;

import com.project.login.model.dto.tag.TagByIdDTO;
import com.project.login.model.request.tag.TagNameRequest;
import com.project.login.model.vo.TagVO;
import com.project.login.model.response.StandardResponse;
import com.project.login.service.tag.TagService;
import com.project.login.convert.TagConvert;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.*;

import jakarta.validation.Valid;

@Tag(name = "Tags", description = "Manage tags")
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/noting/tags")
public class TagController {

    private final TagService tagService;

    @Qualifier("tagConvert")
    private final TagConvert convert;

    @Operation(summary = "Get tag name by ID")
    @PostMapping("/name")
    public StandardResponse<TagVO> getTagName(@Valid @RequestBody TagNameRequest request) {
        TagByIdDTO dto = convert.toByIdDTO(request);
        TagVO vo = tagService.getTagName(dto);
        return StandardResponse.success(vo);
    }
}
