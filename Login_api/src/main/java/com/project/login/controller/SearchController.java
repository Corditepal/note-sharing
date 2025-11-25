package com.project.login.controller;

import com.project.login.convert.SearchConvert;
import com.project.login.model.dto.search.NoteSearchDTO;
import com.project.login.model.request.search.NoteSearchRequest;
import com.project.login.model.response.StandardResponse;
import com.project.login.model.vo.NoteSearchVO;
import com.project.login.service.search.SearchService;
import com.project.login.service.search.SearchService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Tag(name = "Search", description = "Search notes using Elasticsearch")
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/search")
public class SearchController {

    private final SearchService searchService;
    private final SearchConvert convert;

    @Operation(summary = "Search notes by keyword with aggregations and score ranking")
    @PostMapping("/notes")
    public StandardResponse<List<NoteSearchVO>> searchNotes(
            @Valid @RequestBody NoteSearchRequest request
    ) {
        // 1. 转换为 DTO
        NoteSearchDTO dto = convert.toSearchDTO(request);

        // 2. 调用 Service
        List<NoteSearchVO> results = searchService.searchNotes(dto);

        return StandardResponse.success(results);
    }
}
