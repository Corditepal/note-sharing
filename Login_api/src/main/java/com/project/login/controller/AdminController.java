package com.project.login.controller;

import com.project.login.model.entity.UserEntity;
import com.project.login.model.request.sensitive.SensitiveBatchCheckRequest;
import com.project.login.model.request.sensitive.SensitiveTextCheckRequest;
import com.project.login.model.response.StandardResponse;
import com.project.login.model.vo.NoteShowVO;
import com.project.login.model.vo.RemarkDetailVO;
import com.project.login.model.vo.SensitiveCheckResult;
import com.project.login.service.adminservice.OnlineUserService;
import com.project.login.service.noting.NoteService;
import com.project.login.service.remark.RemarkService;
import com.project.login.service.sensitive.DeepFilterService;
import com.project.login.service.sensitive.FastFilterService;
import com.project.login.service.sensitive.ModerationService;
import com.project.login.service.sensitive.SensitiveWordService;
import com.project.login.service.qa.QuestionService;
import com.project.login.model.vo.qa.QuestionVO;
import com.project.login.model.vo.qa.QACommentDetailVO;
import com.project.login.model.vo.qa.CommentVO;
import com.project.login.model.request.moderation.HandleModerationRequest;
import com.project.login.model.request.moderation.SubmitModerationRequest;
import com.project.login.model.request.moderation.ReviewNoteRequest;
import com.project.login.model.vo.NoteModerationVO;
import com.project.login.model.vo.NoteReviewVO;
import com.project.login.model.vo.PendingNoteVO;
import com.project.login.model.vo.SubmitModerationResponseVO;
import com.project.login.model.vo.UserInfoWithNotesAndFollowsVO;
import com.project.login.model.vo.FollowUserVO;
import com.project.login.model.dataobject.NoteModerationDO;
import com.project.login.repository.UserRepository;
import com.project.login.mapper.UserMapper;
import com.project.login.mapper.NoteSpaceMapper;
import com.project.login.mapper.NotebookMapper;
import com.project.login.mapper.NoteMapper;
import com.project.login.mapper.UserFollowMapper;
import com.project.login.model.dataobject.NoteSpaceDO;
import com.project.login.model.dataobject.NotebookDO;
import com.project.login.model.dataobject.NoteDO;
import com.project.login.model.dataobject.UserFollowDO;
import com.project.login.model.dataobject.UserDO;
import com.project.login.model.entity.NoteEntity;
import com.project.login.repository.NoteRepository;
import com.project.login.service.minio.MinioService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Tag(name = "Admin", description = "管理员相关接口")
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/admin")
public class AdminController {

    private final OnlineUserService onlineUserService;
    private final NoteService noteService;
    private final RemarkService remarkService;
    private final SensitiveWordService sensitiveWordService;
    private final FastFilterService fastFilterService;
    private final DeepFilterService deepFilterService;
    private final ModerationService moderationService;
    private final QuestionService questionService;
    private final UserRepository userRepository;
    private final UserMapper userMapper;
    private final NoteSpaceMapper noteSpaceMapper;
    private final NotebookMapper notebookMapper;
    private final NoteMapper noteMapper;
    private final UserFollowMapper userFollowMapper;
    private final NoteRepository noteRepository;
    private final MinioService minioService;

    @Operation(summary = "获取当前所有在线用户")
    @GetMapping("/online-users")
    public StandardResponse<List<Map<String, Object>>> getAllOnlineUsers() {
        List<Map<String, Object>> usersWithLoginTime = onlineUserService.getAllOnlineUsersWithLoginTime();
        
        // 转换为返回格式
        List<Map<String, Object>> userList = new ArrayList<>();
        for (Map<String, Object> userInfo : usersWithLoginTime) {
            UserEntity user = (UserEntity) userInfo.get("user");
            Date loginTime = (Date) userInfo.get("loginTime");
            
            Map<String, Object> userMap = new HashMap<>();
            userMap.put("id", user.getId());
            userMap.put("username", user.getUsername());
            userMap.put("email", user.getEmail());
            userMap.put("studentNumber", user.getStudentNumber() != null ? user.getStudentNumber() : "");
            userMap.put("role", user.getRole() != null ? user.getRole() : "User");
            userMap.put("enabled", user.isEnabled());
            userMap.put("avatarUrl", user.getAvatarUrl() != null ? user.getAvatarUrl() : "");
            // 添加上线时间（TOKEN生效时间）
            userMap.put("loginTime", loginTime != null ? loginTime : null);
            userList.add(userMap);
        }
        
        return StandardResponse.success("获取成功", userList);
    }

    @Operation(summary = "获取所有用户列表（分页）")
    @GetMapping("/users/all")
    public StandardResponse<Map<String, Object>> getAllUsers(
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "30") int size) {
        // 创建分页请求，按ID降序排列
        Pageable pageable = PageRequest.of(page - 1, size, Sort.by(Sort.Direction.DESC, "id"));
        Page<UserEntity> userPage = userRepository.findAll(pageable);
        
        // 转换为返回格式
        List<Map<String, Object>> userList = new ArrayList<>();
        for (UserEntity user : userPage.getContent()) {
            Map<String, Object> userMap = new HashMap<>();
            userMap.put("id", user.getId());
            userMap.put("username", user.getUsername());
            userMap.put("email", user.getEmail());
            userMap.put("studentNumber", user.getStudentNumber() != null ? user.getStudentNumber() : "");
            userMap.put("role", user.getRole() != null ? user.getRole() : "User");
            userMap.put("enabled", user.isEnabled());
            userMap.put("avatarUrl", user.getAvatarUrl() != null ? user.getAvatarUrl() : "");
            userMap.put("createdAt", user.getCreatedAt());
            userMap.put("updatedAt", user.getUpdatedAt());
            userList.add(userMap);
        }
        
        // 构建分页响应
        Map<String, Object> result = new HashMap<>();
        result.put("users", userList);
        result.put("total", userPage.getTotalElements());
        result.put("page", page);
        result.put("size", size);
        result.put("totalPages", userPage.getTotalPages());
        
        return StandardResponse.success("获取成功", result);
    }

    @Operation(summary = "统计笔记总数")
    @GetMapping("/notes/count")
    public StandardResponse<Map<String, Long>> getNoteCount() {
        Long count = noteService.getNoteCount();
        
        Map<String, Long> result = new HashMap<>();
        result.put("noteCount", count);
        
        return StandardResponse.success("获取成功", result);
    }

    @Operation(summary = "获取所有笔记列表（按数据库顺序）")
    @GetMapping("/notes/list")
    public StandardResponse<List<NoteShowVO>> getAllNotes() {
        List<NoteShowVO> notes = noteService.getAllNotes();
        return StandardResponse.success("获取成功", notes);
    }

    @Operation(summary = "统计评论总数")
    @GetMapping("/remarks/count")
    public StandardResponse<Map<String, Long>> getRemarkCount() {
        Long count = remarkService.getRemarkCount();
        
        Map<String, Long> result = new HashMap<>();
        result.put("remarkCount", count);
        
        return StandardResponse.success("获取成功", result);
    }

    @Operation(summary = "获取所有评论列表（按数据库顺序）")
    @GetMapping("/remarks/list")
    public StandardResponse<List<RemarkDetailVO>> getAllRemarks() {
        List<RemarkDetailVO> remarks = remarkService.getAllRemarks();
        return StandardResponse.success("获取成功", remarks);
    }

    @Operation(summary = "管理员删除评论（可删除任何评论）")
    @DeleteMapping("/remarks/{remarkId}")
    public StandardResponse<Boolean> adminDeleteRemark(@PathVariable String remarkId) {
        Boolean result = remarkService.adminDeleteRemark(remarkId);
        if (result) {
            return StandardResponse.success("删除成功", true);
        } else {
            return StandardResponse.error("删除失败");
        }
    }

    // ========== 问答管理 ==========
    @Operation(summary = "统计问题总数")
    @GetMapping("/questions/count")
    public StandardResponse<Map<String, Long>> getQuestionCount() {
        Long count = questionService.getQuestionCount();
        
        Map<String, Long> result = new HashMap<>();
        result.put("questionCount", count);
        
        return StandardResponse.success("获取成功", result);
    }

    @Operation(summary = "获取所有问题列表（按数据库顺序）")
    @GetMapping("/questions/list")
    public StandardResponse<List<QuestionVO>> getAllQuestions() {
        List<QuestionVO> questions = questionService.getAllQuestions();
        return StandardResponse.success("获取成功", questions);
    }

    @Operation(summary = "管理员删除问题（可删除任何问题）")
    @DeleteMapping("/questions/{questionId}")
    public StandardResponse<Void> adminDeleteQuestion(@PathVariable String questionId) {
        questionService.deleteQuestion(questionId);
        return StandardResponse.success("删除成功", null);
    }

    @Operation(summary = "统计回答总数")
    @GetMapping("/answers/count")
    public StandardResponse<Map<String, Long>> getAnswerCount() {
        Long count = questionService.getAnswerCount();
        
        Map<String, Long> result = new HashMap<>();
        result.put("answerCount", count);
        
        return StandardResponse.success("获取成功", result);
    }

    // ========== 问答评论管理 ==========
    @Operation(summary = "统计问答评论总数（包括Comment和Reply）")
    @GetMapping("/qa-comments/count")
    public StandardResponse<Map<String, Long>> getQACommentCount() {
        Long count = questionService.getQACommentCount();
        
        Map<String, Long> result = new HashMap<>();
        result.put("qaCommentCount", count);
        
        return StandardResponse.success("获取成功", result);
    }

    @Operation(summary = "获取所有问答评论列表（包括Comment和Reply，按数据库顺序）")
    @GetMapping("/qa-comments/list")
    public StandardResponse<List<QACommentDetailVO>> getAllQAComments() {
        List<QACommentDetailVO> comments = questionService.getAllQAComments();
        return StandardResponse.success("获取成功", comments);
    }

    @Operation(summary = "管理员删除问答评论（可删除任何评论或回复）")
    @DeleteMapping("/qa-comments/{type}/{id}")
    public StandardResponse<Void> adminDeleteQAComment(
            @PathVariable String type,
            @PathVariable Long id,
            @RequestParam String questionId,
            @RequestParam Long answerId,
            @RequestParam(required = false) Long commentId) {
        if ("COMMENT".equals(type)) {
            // 删除一级评论（会自动删除其下的所有回复）
            questionService.deleteComment(questionId, answerId, id);
        } else if ("REPLY".equals(type)) {
            // 删除二级回复
            if (commentId == null) {
                return StandardResponse.error("删除回复需要提供commentId参数");
            }
            questionService.deleteReply(questionId, answerId, commentId, id);
        } else {
            return StandardResponse.error("无效的评论类型");
        }
        return StandardResponse.success("删除成功", null);
    }

    @Operation(summary = "获取问答评论树（用于查看评论结构）")
    @GetMapping("/qa-comments/tree")
    public StandardResponse<CommentVO> getQACommentTree(
            @RequestParam String questionId,
            @RequestParam Long answerId,
            @RequestParam Long commentId) {
        CommentVO tree = questionService.getCommentTree(questionId, answerId, commentId);
        if (tree == null) {
            return StandardResponse.error("评论不存在");
        }
        return StandardResponse.success("获取成功", tree);
    }

    @Operation(summary = "统计评论下的回复数量")
    @GetMapping("/qa-comments/count-replies")
    public StandardResponse<Map<String, Integer>> countCommentReplies(
            @RequestParam String questionId,
            @RequestParam Long answerId,
            @RequestParam Long commentId) {
        int count = questionService.countCommentReplies(questionId, answerId, commentId);
        Map<String, Integer> result = new HashMap<>();
        result.put("replyCount", count);
        return StandardResponse.success("获取成功", result);
    }

    @Operation(summary = "检查纯文本敏感词")
    @PostMapping("/sensitive/check/text")
    public StandardResponse<SensitiveCheckResult> checkText(@RequestBody SensitiveTextCheckRequest request) {
        SensitiveCheckResult result = sensitiveWordService.checkText(request.getText());
        return StandardResponse.success("检查完成", result);
    }

    @Operation(summary = "检查笔记敏感词（全文模式）")
    @GetMapping("/sensitive/check/note/{noteId}")
    public StandardResponse<SensitiveCheckResult> checkNote(@PathVariable Long noteId) {
        SensitiveCheckResult result = sensitiveWordService.checkNote(noteId);
        return StandardResponse.success("检查完成", result);
    }

    @Operation(summary = "检查笔记敏感词（全文模式，兼容旧接口）")
    @GetMapping("/sensitive/check/note/{noteId}/full")
    public StandardResponse<SensitiveCheckResult> checkNoteFull(@PathVariable Long noteId) {
        SensitiveCheckResult result = sensitiveWordService.checkNote(noteId);
        return StandardResponse.success("检查完成", result);
    }

    @Operation(summary = "批量检查笔记敏感词")
    @PostMapping("/sensitive/check/batch")
    public StandardResponse<Map<String, Object>> batchCheckNotes(@RequestBody SensitiveBatchCheckRequest request) {
        List<Long> noteIds = request.getNoteIds();
        Boolean full = request.getFull() != null ? request.getFull() : false;
        // 注意：concurrency 参数目前未使用，未来可用于并发控制
        
        Map<String, Object> results = new HashMap<>();
        List<Map<String, Object>> checkResults = new ArrayList<>();
        
        for (Long noteId : noteIds) {
            try {
                SensitiveCheckResult result = sensitiveWordService.checkNote(noteId, full);
                Map<String, Object> item = new HashMap<>();
                item.put("noteId", noteId);
                item.put("status", result.getStatus());
                item.put("riskLevel", result.getRiskLevel());
                item.put("score", result.getScore());
                checkResults.add(item);
            } catch (Exception e) {
                Map<String, Object> item = new HashMap<>();
                item.put("noteId", noteId);
                item.put("status", "ERROR");
                item.put("message", e.getMessage());
                checkResults.add(item);
            }
        }
        
        results.put("total", noteIds.size());
        results.put("results", checkResults);
        
        return StandardResponse.success("批量检查完成", results);
    }

    @Operation(summary = "快速过滤检查文本（使用Trie树）")
    @PostMapping("/sensitive/fast-filter")
    public StandardResponse<Map<String, Object>> fastFilter(@RequestBody Map<String, String> request) {
        String text = request.get("text");
        if (text == null || text.isEmpty()) {
            return StandardResponse.error("文本不能为空");
        }
        
        List<String> hits = fastFilterService.match(text);
        Map<String, Object> result = new HashMap<>();
        result.put("hasSensitiveWords", !hits.isEmpty());
        result.put("hits", hits);
        result.put("hitCount", hits.size());
        
        return StandardResponse.success("快速过滤完成", result);
    }

    @Operation(summary = "重新加载敏感词库")
    @PostMapping("/sensitive/reload")
    public StandardResponse<Map<String, String>> reloadSensitiveWords() {
        fastFilterService.reload();
        Map<String, String> result = new HashMap<>();
        result.put("message", "敏感词库已重新加载");
        return StandardResponse.success("重新加载成功", result);
    }

    @Operation(summary = "深度检查文本（使用keywords.txt完整词库）")
    @PostMapping("/sensitive/deep-check")
    public StandardResponse<Map<String, Object>> deepCheck(@RequestBody Map<String, String> request) {
        String text = request.get("text");
        if (text == null || text.isEmpty()) {
            return StandardResponse.error("文本不能为空");
        }
        
        Map<String, Object> result = deepFilterService.deepCheck(text);
        return StandardResponse.success("深度检查完成", result);
    }

    @Operation(summary = "重新加载深度检查词库（keywords.txt）")
    @PostMapping("/sensitive/deep-reload")
    public StandardResponse<Map<String, String>> reloadDeepFilter() {
        deepFilterService.reload();
        Map<String, String> result = new HashMap<>();
        result.put("message", "深度检查词库已重新加载");
        return StandardResponse.success("重新加载成功", result);
    }

    @Operation(summary = "获取所有审查记录列表（包括已处理和未处理的，用于留档）")
    @GetMapping("/moderation/pending")
    public StandardResponse<List<NoteModerationVO>> getPendingModerations() {
        List<NoteModerationVO> moderations = moderationService.getAllFlagged();
        return StandardResponse.success("获取成功", moderations);
    }

    @Operation(summary = "获取待审核的笔记列表")
    @GetMapping("/moderation/pending-notes")
    public StandardResponse<List<PendingNoteVO>> getPendingNotes() {
        List<PendingNoteVO> notes = moderationService.getPendingNotes();
        return StandardResponse.success("获取成功", notes);
    }

    @Operation(summary = "获取审查记录详情")
    @GetMapping("/moderation/{id}")
    public StandardResponse<NoteModerationVO> getModerationById(@PathVariable Long id) {
        NoteModerationVO moderation = moderationService.getById(id);
        if (moderation == null) {
            return StandardResponse.error("审查记录不存在");
        }
        return StandardResponse.success("获取成功", moderation);
    }

    @Operation(summary = "获取笔记的审查历史")
    @GetMapping("/moderation/note/{noteId}")
    public StandardResponse<List<NoteModerationVO>> getModerationsByNoteId(@PathVariable Long noteId) {
        List<NoteModerationVO> moderations = moderationService.getByNoteId(noteId);
        return StandardResponse.success("获取成功", moderations);
    }

    @Operation(summary = "提交审查记录（标记笔记为审核中）")
    @PostMapping("/moderation/submit")
    public StandardResponse<SubmitModerationResponseVO> submitModeration(
            @RequestBody SubmitModerationRequest request) {
        try {
            // 构造 SensitiveCheckResult 对象
            SensitiveCheckResult result = new SensitiveCheckResult();
            // 提交审查时，状态必须为FLAGGED，确保能被管理员端查询到
            result.setStatus("FLAGGED");
            result.setRiskLevel(request.getRiskLevel());
            result.setScore(request.getScore());
            result.setCategories(request.getCategories());
            
            // 转换 findings
            if (request.getFindings() != null) {
                List<SensitiveCheckResult.Finding> findings = new ArrayList<>();
                for (SubmitModerationRequest.Finding f : request.getFindings()) {
                    SensitiveCheckResult.Finding finding = new SensitiveCheckResult.Finding();
                    finding.setTerm(f.getTerm());
                    finding.setCategory(f.getCategory());
                    finding.setConfidence(f.getConfidence());
                    finding.setSnippet(f.getSnippet());
                    finding.setStartOffset(f.getStartOffset());
                    finding.setEndOffset(f.getEndOffset());
                    findings.add(finding);
                }
                result.setFindings(findings);
            }
            
            // 设置笔记元信息
            SensitiveCheckResult.NoteMeta noteMeta = new SensitiveCheckResult.NoteMeta();
            noteMeta.setNoteId(request.getNoteId());
            result.setNoteMeta(noteMeta);
            
            // 保存审查记录并获取审查记录DO对象（包含ID和创建时间）
            NoteModerationDO moderationDO = moderationService.saveResultAndReturnDO(result);
            
            // 构建响应对象
            SubmitModerationResponseVO response = SubmitModerationResponseVO.builder()
                    .moderationId(moderationDO.getId())
                    .contentType("NOTE")  // 内容类型固定为NOTE
                    .contentId(request.getNoteId())
                    .status("FLAGGED")  // 提交审查时状态固定为FLAGGED
                    .createdAt(moderationDO.getCreatedAt())
                    .build();
            
            return StandardResponse.success("审查记录已提交", response);
        } catch (Exception e) {
            log.error("提交审查记录失败", e);
            return StandardResponse.error("提交审查记录失败: " + e.getMessage());
        }
    }

    @Operation(summary = "审查笔记（调用深度检测）")
    @GetMapping("/moderation/review/{noteId}")
    public StandardResponse<NoteReviewVO> reviewNote(@PathVariable Long noteId) {
        try {
            NoteReviewVO reviewResult = moderationService.reviewNote(noteId);
            return StandardResponse.success("审查完成", reviewResult);
        } catch (RuntimeException e) {
            return StandardResponse.error(e.getMessage());
        }
    }

    @Operation(summary = "处理审查结果（通过/未通过，发布/退回，发送私信）")
    @PostMapping("/moderation/review-result")
    public StandardResponse<Map<String, String>> handleReviewResult(
            @RequestBody ReviewNoteRequest request) {
        try {
            moderationService.handleReviewResult(
                    request.getModerationId(),
                    request.getApproved(),
                    request.getAdminComment()
            );
            Map<String, String> result = new HashMap<>();
            result.put("message", request.getApproved() ? "审查通过，笔记已发布" : "审查未通过，笔记已退回");
            return StandardResponse.success("处理成功", result);
        } catch (RuntimeException e) {
            return StandardResponse.error(e.getMessage());
        }
    }

    @Operation(summary = "处理审查记录（标记为已处理并添加备注）")
    @PostMapping("/moderation/{id}/handle")
    public StandardResponse<Map<String, String>> handleModeration(
            @PathVariable Long id,
            @RequestBody HandleModerationRequest request) {
        try {
            moderationService.handleModeration(
                    id,
                    request.getIsHandled() != null ? request.getIsHandled() : true,
                    request.getAdminComment()
            );
            Map<String, String> result = new HashMap<>();
            result.put("message", "处理成功");
            return StandardResponse.success("处理成功", result);
        } catch (RuntimeException e) {
            return StandardResponse.error(e.getMessage());
        }
    }

    @Operation(summary = "根据邮箱获取用户的所有发布文章和关注列表")
    @GetMapping("/user/by-email")
    public StandardResponse<UserInfoWithNotesAndFollowsVO> getUserInfoByEmail(@RequestParam String email) {
        try {
            // 1. 根据邮箱查找用户
            UserEntity userEntity = userRepository.findByEmail(email)
                    .orElseThrow(() -> new RuntimeException("用户不存在"));
            
            Long userId = userEntity.getId();
            
            // 2. 获取用户的所有笔记空间
            List<NoteSpaceDO> spaces = noteSpaceMapper.selectByUser(userId);
            
            // 3. 获取所有笔记本
            List<Long> notebookIds = new ArrayList<>();
            for (NoteSpaceDO space : spaces) {
                List<NotebookDO> notebooks = notebookMapper.selectBySpaceId(space.getId());
                for (NotebookDO notebook : notebooks) {
                    notebookIds.add(notebook.getId());
                }
            }
            
            // 4. 获取所有笔记
            List<NoteDO> allNotes = new ArrayList<>();
            for (Long notebookId : notebookIds) {
                List<NoteDO> notes = noteMapper.selectByNotebookId(notebookId);
                allNotes.addAll(notes);
            }
            
            // 5. 过滤出已发布的笔记（在note_stats中有记录，且在Elasticsearch中有记录）
            List<NoteDO> publishedNotes = new ArrayList<>();
            if (!allNotes.isEmpty()) {
                // 查询note_stats表，获取已发布的笔记ID
                List<Long> noteIds = allNotes.stream().map(NoteDO::getId).collect(Collectors.toList());
                
                // 查询Elasticsearch，获取已发布的笔记ID列表
                List<Long> publishedNoteIds = new ArrayList<>();
                try {
                    Iterable<NoteEntity> esEntities = noteRepository.findAllById(noteIds);
                    for (NoteEntity entity : esEntities) {
                        if (entity != null && entity.getId() != null) {
                            publishedNoteIds.add(entity.getId());
                        }
                    }
                } catch (Exception e) {
                    log.error("查询ES中的笔记失败", e);
                }
                
                // 过滤出已发布的笔记
                publishedNotes = allNotes.stream()
                        .filter(note -> publishedNoteIds.contains(note.getId()))
                        .collect(Collectors.toList());
            }
            
            // 6. 转换为NoteShowVO列表
            List<NoteShowVO> noteShowVOList = new ArrayList<>();
            for (NoteDO noteDO : publishedNotes) {
                NoteShowVO vo = new NoteShowVO();
                vo.setId(noteDO.getId());
                vo.setTitle(noteDO.getTitle());
                vo.setFileType(noteDO.getFileType());
                vo.setNotebookId(noteDO.getNotebookId());
                vo.setCreatedAt(noteDO.getCreatedAt());
                vo.setUpdatedAt(noteDO.getUpdatedAt());
                // 获取文件访问URL
                if (noteDO.getFilename() != null && !noteDO.getFilename().isEmpty()) {
                    String url = minioService.getFileUrl(noteDO.getFilename());
                    vo.setUrl(url);
                }
                // 设置作者信息
                vo.setAuthorName(userEntity.getUsername());
                vo.setAuthorEmail(userEntity.getEmail());
                noteShowVOList.add(vo);
            }
            
            // 7. 获取关注列表
            List<UserFollowDO> followings = userFollowMapper.selectFollowings(userId);
            List<FollowUserVO> followUserVOList = new ArrayList<>();
            for (UserFollowDO follow : followings) {
                try {
                    UserDO followee = userMapper.selectById(follow.getFolloweeId());
                    if (followee != null) {
                        FollowUserVO followVO = FollowUserVO.builder()
                                .userId(followee.getId())
                                .username(followee.getUsername())
                                .email(followee.getEmail())
                                .avatarUrl(followee.getAvatarUrl())
                                .followTime(follow.getFollowTime())
                                .build();
                        followUserVOList.add(followVO);
                    }
                } catch (Exception e) {
                    log.warn("获取被关注用户信息失败 followeeId={}", follow.getFolloweeId(), e);
                }
            }
            
            // 8. 构建响应对象
            UserInfoWithNotesAndFollowsVO result = UserInfoWithNotesAndFollowsVO.builder()
                    .userId(userEntity.getId())
                    .username(userEntity.getUsername())
                    .email(userEntity.getEmail())
                    .studentNumber(userEntity.getStudentNumber())
                    .avatarUrl(userEntity.getAvatarUrl())
                    .publishedNotes(noteShowVOList)
                    .followings(followUserVOList)
                    .build();
            
            return StandardResponse.success("获取成功", result);
        } catch (RuntimeException e) {
            log.error("根据邮箱获取用户信息失败 email={}", email, e);
            return StandardResponse.error(e.getMessage());
        } catch (Exception e) {
            log.error("根据邮箱获取用户信息失败 email={}", email, e);
            return StandardResponse.error("获取用户信息失败: " + e.getMessage());
        }
    }
}
