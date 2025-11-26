package com.project.login.controller.admin;

import com.project.login.model.request.admin.UserAdminUpdateRequest;
import com.project.login.model.entity.UserEntity;
import com.project.login.model.response.admin.UserAdminVO;
import com.project.login.repository.UserRepository;
import com.project.login.service.admin.AdminUserService;
import com.project.login.model.response.StandardResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.stream.Collectors;

@Tag(name = "Admin Users", description = "Admin user management")
@SecurityRequirement(name = "BearerAuth")
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/admin/users")
public class AdminUserController {

    private final UserRepository userRepository;
    private final AdminUserService adminUserService;

    @Operation(summary = "List users by studentId keyword")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "成功",
                    content = @Content(mediaType = "application/json",
                            schema = @Schema(implementation = StandardResponse.class),
                            examples = @ExampleObject(value = "{\n  \"code\": 200,\n  \"message\": \"success\",\n  \"data\": [\n    {\n      \"id\": 1,\n      \"username\": \"alice\",\n      \"studentId\": \"20250001\",\n      \"email\": \"alice@example.com\",\n      \"createdAt\": \"2025-01-01T12:00:00Z\"\n    }\n  ]\n}")))
    })
    @GetMapping
    public ResponseEntity<StandardResponse<?>> list(@RequestParam(value = "studentId", required = false) String studentId) {
        List<UserEntity> users = studentId == null || studentId.isBlank()
                ? userRepository.findAll()
                : userRepository.findByStudentNumberContaining(studentId);

        List<UserAdminVO> data = users.stream().map(UserAdminVO::from).collect(Collectors.toList());
        return ResponseEntity.ok(StandardResponse.success(data));
    }

    @Operation(summary = "Update user basic info")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "成功",
                    content = @Content(mediaType = "application/json",
                            schema = @Schema(implementation = StandardResponse.class)))
    })
    @PutMapping("/{id}")
    public ResponseEntity<StandardResponse<?>> update(@PathVariable("id") Long id, @RequestBody UserAdminUpdateRequest req) {
        UserEntity updated = adminUserService.updateUser(id, req);
        return ResponseEntity.ok(StandardResponse.success(UserAdminVO.from(updated)));
    }

    @Operation(summary = "Delete user and related spaces")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "成功",
                    content = @Content(mediaType = "application/json",
                            schema = @Schema(implementation = StandardResponse.class)))
    })
    @DeleteMapping("/{id}")
    public ResponseEntity<StandardResponse<?>> delete(@PathVariable("id") Long id) {
        adminUserService.deleteUser(id);
        return ResponseEntity.ok(StandardResponse.success("删除成功", null));
    }
}