package com.project.login.controller.admin;

import com.project.login.model.response.admin.CommentAdminVO;
import com.project.login.mapper.CommentMapper;
import com.project.login.model.response.StandardResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import lombok.RequiredArgsConstructor;
import com.github.pagehelper.page.PageMethod;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

@Tag(name = "Admin Comments", description = "Admin comment management")
@SecurityRequirement(name = "BearerAuth")
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/admin/comments")
public class AdminCommentController {

    private final CommentMapper commentMapper;

    @Operation(summary = "Query comments by date and publisher")
    @ApiResponse(responseCode = "200", description = "成功",
            content = @Content(mediaType = "application/json",
                    schema = @Schema(implementation = StandardResponse.class)))
    @GetMapping
    public ResponseEntity<StandardResponse<java.util.List<com.project.login.model.response.admin.CommentAdminVO>>> list(@RequestParam(value = "date", required = false)
                                  @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date,
                                  @RequestParam(value = "publisher", required = false) String publisher,
                                  @RequestParam(value = "page", required = false, defaultValue = "1") int page,
                                  @RequestParam(value = "size", required = false, defaultValue = "20") int size) {
        LocalDateTime start = null;
        LocalDateTime end = null;
        if (date != null) {
            start = date.atStartOfDay();
            end = date.plusDays(1).atStartOfDay();
        }
        PageMethod.startPage(page, size);
        List<CommentAdminVO> data = commentMapper.search(publisher, start, end);
        return ResponseEntity.ok(StandardResponse.success(data));
    }

    @Operation(summary = "Delete a comment")
    @ApiResponse(responseCode = "200", description = "成功",
            content = @Content(mediaType = "application/json",
                    schema = @Schema(implementation = StandardResponse.class)))
    @DeleteMapping("/{id}")
    public ResponseEntity<StandardResponse<String>> delete(@PathVariable Long id) {
        commentMapper.deleteById(id);
        return ResponseEntity.ok(StandardResponse.success("删除成功", null));
    }
}
