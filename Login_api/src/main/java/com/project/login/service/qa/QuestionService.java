package com.project.login.service.qa;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.login.convert.QuestionConvert;
import com.project.login.model.dataobject.QuestionDO;
import com.project.login.model.dto.qa.*;
import com.project.login.model.event.QuestionEvent;
import com.project.login.model.vo.qa.AnswerVO;
import com.project.login.model.vo.qa.CommentVO;
import com.project.login.model.vo.qa.QuestionVO;
import com.project.login.model.vo.qa.ReplyVO;
import com.project.login.model.vo.qa.QACommentDetailVO;
import com.project.login.mapper.UserMapper;
import com.project.login.repository.QuestionRepository;

import com.project.login.service.notification.NotificationService;
import jakarta.annotation.Resource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class QuestionService {

    private final QuestionRepository repo;
    private final QuestionConvert convert;
    private final RabbitTemplate rabbitTemplate;
    private final ObjectMapper objectMapper;
    private final NotificationService notificationService;
    private final UserMapper userMapper;
    @Resource
    private StringRedisTemplate redis;


    private void updateRedisCacheIfExists(String questionId, QuestionDO updated) {
        String key = "question:detail:" + questionId;

        Boolean exists = redis.hasKey(key);
        if (Boolean.TRUE.equals(exists)) {
            try {
                String json = objectMapper.writeValueAsString(updated);
                redis.opsForValue().set(key, json, Duration.ofHours(2));
            } catch (Exception e) {
                log.error("更新 Redis 缓存失败 key={}", key, e);
            }
        }
    }


    /** 创建问题并异步发送到 Elasticsearch */
    public QuestionVO createQuestion(QuestionCreateDTO dto) {
        // 1. 构建 QuestionDO 并保存到 MongoDB
        QuestionDO q = new QuestionDO();
        q.setAuthorId(dto.getAuthorId());
        q.setTitle(dto.getTitle());
        q.setContent(dto.getContent());
        q.setTags(dto.getTags());
        q.setCreatedAt(LocalDateTime.now());

        // 保存问题，Mongo 自动生成 _id
        repo.save(q);

        // 2. 异步发送到 Elasticsearch
        String tagsStr = String.join(",", q.getTags()); // 标签列表转成字符串
        QuestionEvent event = new QuestionEvent(
                q.getQuestionId(),
                q.getTitle(),
                q.getContent(),
                tagsStr,
                QuestionEvent.EventType.CREATE
        );
        rabbitTemplate.convertAndSend("question.es.queue", event);

        // 3. 给粉丝发送“我关注的人发布了问题”的通知
        notificationService.createQuestionPublishNotifications(q.getQuestionId());

        // 4. 返回 VO
        return convert.toQuestionVO(q);
    }


    /** 创建回答 */
    public AnswerVO createAnswer(AnswerCreateDTO dto) {
        QuestionDO q = repo.findByQuestionId(dto.getQuestionId());
        if (q == null) return null;

        QuestionDO.AnswerDO a = new QuestionDO.AnswerDO();
        a.setAnswerId(System.currentTimeMillis());
        a.setAuthorId(dto.getAuthorId());
        a.setContent(dto.getContent());
        a.setCreatedAt(LocalDateTime.now());

        q.getAnswers().add(a);
        repo.save(q);

        updateRedisCacheIfExists(dto.getQuestionId(), q);

        AnswerVO vo = convert.toAnswerVO(a);

        // 给提问者发送“回答了我的问题”的通知
        notificationService.createAnswerNotification(dto.getAuthorId(), dto.getQuestionId(), a.getAnswerId());

        return vo;
    }

    /** 创建评论 */
    public CommentVO createComment(CommentCreateDTO dto) {
        QuestionDO q = repo.findByQuestionId(dto.getQuestionId());
        if (q == null) return null;

        // 查找指定回答
        QuestionDO.AnswerDO answer = q.getAnswers().stream()
                .filter(a -> a.getAnswerId().equals(dto.getAnswerId()))
                .findFirst()
                .orElse(null);

        if (answer == null) return null;

        // 创建新的评论
        QuestionDO.CommentDO comment = new QuestionDO.CommentDO();
        comment.setCommentId(System.currentTimeMillis()); // 可以换成自动生成的ID
        comment.setAuthorId(dto.getAuthorId());
        comment.setContent(dto.getContent());
        comment.setCreatedAt(LocalDateTime.now());

        // 将评论添加到对应回答的评论列表
        answer.getComments().add(comment);

        // 保存更新后的问题对象
        repo.save(q);

        updateRedisCacheIfExists(dto.getQuestionId(), q);

        // 转换为 VO 对象并返回
        CommentVO vo = convert.toCommentVO(comment);

        // 给回答作者发送“评论我的回答”的通知
        notificationService.createQuestionCommentNotification(dto.getAuthorId(), dto.getQuestionId(), dto.getAnswerId());

        return vo;
    }


    /** 创建回复（二级评论） */
    public ReplyVO createReply(ReplyCreateDTO dto) {
        // 查找问题
        QuestionDO q = repo.findByQuestionId(dto.getQuestionId());
        if (q == null) return null; // 如果问题不存在，返回 null

        // 查找指定的回答
        QuestionDO.AnswerDO answer = q.getAnswers().stream()
                .filter(a -> a.getAnswerId().equals(dto.getAnswerId()))
                .findFirst()
                .orElse(null);
        if (answer == null) return null; // 如果回答不存在，返回 null

        // 查找指定的评论
        QuestionDO.CommentDO comment = answer.getComments().stream()
                .filter(c -> c.getCommentId().equals(dto.getCommentId()))
                .findFirst()
                .orElse(null);
        if (comment == null) return null; // 如果评论不存在，返回 null

        // 创建新的回复
        QuestionDO.ReplyDO reply = new QuestionDO.ReplyDO();
        reply.setReplyId(System.currentTimeMillis()); // 可以换成自动生成的ID
        reply.setAuthorId(dto.getAuthorId());
        reply.setContent(dto.getContent());
        reply.setCreatedAt(LocalDateTime.now());

        // 将回复添加到评论的回复列表中
        comment.getReplies().add(reply);

        // 保存更新后的问题对象
        repo.save(q);

        updateRedisCacheIfExists(dto.getQuestionId(), q);

        // 转换为 VO 对象并返回
        ReplyVO vo = convert.toReplyVO(reply);

        // 给被回复的评论作者发送“回复我的评论”的通知
        notificationService.createQuestionReplyNotification(dto.getAuthorId(), dto.getQuestionId(), dto.getAnswerId(), dto.getCommentId());

        return vo;
    }


    /** 点赞问题 */
    public void likeQuestion(Long userId, String questionId) {
        QuestionDO q = repo.findByQuestionId(questionId);
        if (q == null) return;

        boolean alreadyLiked = q.getLikes().contains(userId);
        if (alreadyLiked) {
            q.getLikes().remove(userId);
        } else {
            q.getLikes().add(userId);
        }

        repo.save(q);

        updateRedisCacheIfExists(questionId, q);

        // 只有新点赞时才发送通知
        if (!alreadyLiked) {
            notificationService.createQuestionLikeNotification(userId, questionId);
        }
    }

    /** 点赞回答 */
    public void likeAnswer(Long userId, String questionId, Long answerId) {
        QuestionDO q = repo.findByQuestionId(questionId);
        if (q == null) return;

        // 查找指定回答
        QuestionDO.AnswerDO answer = q.getAnswers().stream()
                .filter(a -> a.getAnswerId().equals(answerId))
                .findFirst()
                .orElse(null);

        if (answer == null) return; // 如果没有找到该回答

        // 判断用户是否已点赞
        boolean alreadyLiked = answer.getLikes().contains(userId);
        if (alreadyLiked) {
            answer.getLikes().remove(userId); // 如果已点赞，则移除
        } else {
            answer.getLikes().add(userId);    // 如果未点赞，则添加
        }

        // 保存更新后的问题
        repo.save(q);

        updateRedisCacheIfExists(questionId, q);

        if (!alreadyLiked) {
            notificationService.createAnswerLikeNotification(userId, questionId, answerId);
        }
    }

    /** 点赞评论 */
    public void likeComment(Long userId, String questionId, Long answerId, Long commentId) {
        QuestionDO q = repo.findByQuestionId(questionId);
        if (q == null) return;

        // 查找指定回答
        QuestionDO.AnswerDO answer = q.getAnswers().stream()
                .filter(a -> a.getAnswerId().equals(answerId))
                .findFirst()
                .orElse(null);

        if (answer == null) return; // 如果没有找到该回答

        // 查找指定评论
        QuestionDO.CommentDO comment = answer.getComments().stream()
                .filter(c -> c.getCommentId().equals(commentId))
                .findFirst()
                .orElse(null);

        if (comment == null) return; // 如果没有找到该评论

        // 判断用户是否已点赞
        boolean alreadyLiked = comment.getLikes().contains(userId);
        if (alreadyLiked) {
            comment.getLikes().remove(userId); // 如果已点赞，则移除
        } else {
            comment.getLikes().add(userId);    // 如果未点赞，则添加
        }

        // 保存更新后的问题
        repo.save(q);

        updateRedisCacheIfExists(questionId, q);

        if (!alreadyLiked) {
            notificationService.createQuestionCommentLikeNotification(userId, questionId, answerId, commentId);
        }
    }

    /** 点赞回复（二级评论） */
    public void likeReply(Long userId, String questionId, Long answerId, Long commentId, Long replyId) {
        QuestionDO q = repo.findByQuestionId(questionId);
        if (q == null) return;

        // 查找指定回答
        QuestionDO.AnswerDO answer = q.getAnswers().stream()
                .filter(a -> a.getAnswerId().equals(answerId))
                .findFirst()
                .orElse(null);

        if (answer == null) return; // 如果没有找到该回答

        // 查找指定评论
        QuestionDO.CommentDO comment = answer.getComments().stream()
                .filter(c -> c.getCommentId().equals(commentId))
                .findFirst()
                .orElse(null);

        if (comment == null) return; // 如果没有找到该评论

        // 查找指定回复
        QuestionDO.ReplyDO reply = comment.getReplies().stream()
                .filter(r -> r.getReplyId().equals(replyId))
                .findFirst()
                .orElse(null);

        if (reply == null) return; // 如果没有找到该回复

        // 判断用户是否已点赞
        boolean alreadyLiked = reply.getLikes().contains(userId);
        if (alreadyLiked) {
            reply.getLikes().remove(userId); // 如果已点赞，则移除
        } else {
            reply.getLikes().add(userId);    // 如果未点赞，则添加
        }

        // 保存更新后的问题
        repo.save(q);

        updateRedisCacheIfExists(questionId, q);

        if (!alreadyLiked) {
            notificationService.createQuestionReplyLikeNotification(userId, questionId, answerId, commentId, replyId);
        }
    }


    /** 收藏问题 */
    public void favoriteQuestion(Long userId, String questionId) {
        QuestionDO q = repo.findByQuestionId(questionId);
        if (q == null) return;

        boolean alreadyFavorite = q.getFavorites().contains(userId);
        if (alreadyFavorite)
            q.getFavorites().remove(userId);
        else
            q.getFavorites().add(userId);

        repo.save(q);

        updateRedisCacheIfExists(questionId, q);

        if (!alreadyFavorite) {
            notificationService.createQuestionFavoriteNotification(userId, questionId);
        }
    }

    // 删除操作

    /** 删除提问并异步发送到 Elasticsearch */
    public void deleteQuestion(String questionId) {
        // 1. 查询问题，确保获取到标题、内容、标签等信息
        QuestionDO q = repo.findByQuestionId(questionId);
        if (q == null) return;

        // 2. 删除 MongoDB 中的记录
        repo.deleteByQuestionId(questionId);

        redis.delete("question:detail:" + questionId);

        // 3. 异步发送删除事件到 Elasticsearch
        String tagsStr = String.join(",", q.getTags());
        QuestionEvent event = new QuestionEvent(
                q.getQuestionId(),
                q.getTitle(),
                q.getContent(),
                tagsStr,
                QuestionEvent.EventType.DELETE
        );
        rabbitTemplate.convertAndSend("question.es.queue", event);
    }


    /** 删除回答 */
    public void deleteAnswer(String questionId, Long answerId) {
        QuestionDO q = repo.findByQuestionId(questionId);
        if (q == null) return;

        q.getAnswers().removeIf(a -> a.getAnswerId().equals(answerId));
        repo.save(q);

        updateRedisCacheIfExists(questionId, q);
    }

    /** 删除评论（同时删除其下的所有回复） */
    public void deleteComment(String questionId, Long answerId, Long commentId) {
        QuestionDO q = repo.findByQuestionId(questionId);
        if (q == null) return;

        q.getAnswers().stream()
                .filter(a -> a.getAnswerId().equals(answerId))
                .findFirst()
                .ifPresent(a -> {
                    // 查找要删除的评论
                    QuestionDO.CommentDO commentToDelete = a.getComments().stream()
                            .filter(c -> c.getCommentId().equals(commentId))
                            .findFirst()
                            .orElse(null);
                    
                    if (commentToDelete != null) {
                        // 删除评论下的所有回复（replies会被自动删除，因为Comment被删除）
                        // 这里直接删除整个Comment对象即可，其下的replies会自动删除
                        a.getComments().removeIf(c -> c.getCommentId().equals(commentId));
                    }
                });

        repo.save(q);

        updateRedisCacheIfExists(questionId, q);
    }

    /** 删除回复（二级评论） */
    public void deleteReply(String questionId, Long answerId, Long commentId, Long replyId) {
        QuestionDO q = repo.findByQuestionId(questionId);
        if (q == null) return;

        q.getAnswers().stream()
                .filter(a -> a.getAnswerId().equals(answerId))
                .flatMap(a -> a.getComments().stream())
                .filter(c -> c.getCommentId().equals(commentId))
                .findFirst()
                .ifPresent(c ->
                        c.getReplies().removeIf(r -> r.getReplyId().equals(replyId))
                );

        repo.save(q);

        updateRedisCacheIfExists(questionId, q);
    }

    /** 查询详情（带 Redis 缓存） */
    public QuestionVO getQuestion(String questionId) {
        String key = "question:detail:" + questionId;

        // 1. 查 Redis
        String cacheJson = redis.opsForValue().get(key);
        if (cacheJson != null) {
            try {
                QuestionDO q = objectMapper.readValue(cacheJson, QuestionDO.class);
                return convert.toQuestionVO(q);
            } catch (Exception e) {
                log.warn("Redis 缓存 JSON 解析失败，降级读取 Mongo，key={}", key, e);
            }
        }

        // 2. Mongo 查询
        QuestionDO q = repo.findByQuestionId(questionId);
        if (q == null) return null;

        // 3. 同步写入 Redis，设置生存时间 1 小时
        try {
            String json = objectMapper.writeValueAsString(q);

            redis.opsForValue().set(
                    key,
                    json,
                    Duration.ofHours(2)        // ← TTL = 2 小时
            );

        } catch (Exception e) {
            log.error("写入 Redis 缓存失败 questionId={}", questionId, e);
        }

        return convert.toQuestionVO(q);
    }

    /** 统计问题总数（管理员用） */
    public Long getQuestionCount() {
        return repo.count();
    }

    /** 统计回答总数（管理员用） */
    public Long getAnswerCount() {
        List<QuestionDO> doList = repo.findAll();
        long count = 0;
        for (QuestionDO q : doList) {
            if (q.getAnswers() != null) {
                count += q.getAnswers().size();
            }
        }
        return count;
    }

    /** 获取所有问题列表（管理员用） */
    public List<QuestionVO> getAllQuestions() {
        List<QuestionDO> doList = repo.findAll();
        if (doList.isEmpty()) {
            return new ArrayList<>();
        }
        
        List<QuestionVO> voList = new ArrayList<>();
        for (QuestionDO q : doList) {
            QuestionVO vo = convert.toQuestionVO(q);
            voList.add(vo);
        }
        
        return voList;
    }

    /** 统计问答评论总数（包括Comment和Reply，管理员用） */
    public Long getQACommentCount() {
        List<QuestionDO> doList = repo.findAll();
        long count = 0;
        for (QuestionDO q : doList) {
            if (q.getAnswers() != null) {
                for (QuestionDO.AnswerDO answer : q.getAnswers()) {
                    if (answer.getComments() != null) {
                        count += answer.getComments().size(); // 一级评论
                        // 统计二级回复
                        for (QuestionDO.CommentDO comment : answer.getComments()) {
                            if (comment.getReplies() != null) {
                                count += comment.getReplies().size();
                            }
                        }
                    }
                }
            }
        }
        return count;
    }

    /** 获取所有问答评论列表（包括Comment和Reply，管理员用） */
    public List<QACommentDetailVO> getAllQAComments() {
        List<QuestionDO> doList = repo.findAll();
        List<QACommentDetailVO> voList = new ArrayList<>();
        
        for (QuestionDO q : doList) {
            String questionId = q.getQuestionId();
            String questionTitle = q.getTitle();
            
            if (q.getAnswers() != null) {
                for (QuestionDO.AnswerDO answer : q.getAnswers()) {
                    Long answerId = answer.getAnswerId();
                    
                    // 处理一级评论（Comment）
                    if (answer.getComments() != null) {
                        for (QuestionDO.CommentDO comment : answer.getComments()) {
                            QACommentDetailVO vo = QACommentDetailVO.builder()
                                    .id(comment.getCommentId())
                                    .type("COMMENT")
                                    .questionId(questionId)
                                    .questionTitle(questionTitle)
                                    .answerId(answerId)
                                    .parentCommentId(null)
                                    .authorId(comment.getAuthorId())
                                    .authorName(getAuthorName(comment.getAuthorId()))
                                    .content(comment.getContent())
                                    .createdAt(comment.getCreatedAt())
                                    .likeCount(comment.getLikes() != null ? comment.getLikes().size() : 0)
                                    .build();
                            voList.add(vo);
                            
                            // 处理二级回复（Reply）
                            if (comment.getReplies() != null) {
                                for (QuestionDO.ReplyDO reply : comment.getReplies()) {
                                    QACommentDetailVO replyVo = QACommentDetailVO.builder()
                                            .id(reply.getReplyId())
                                            .type("REPLY")
                                            .questionId(questionId)
                                            .questionTitle(questionTitle)
                                            .answerId(answerId)
                                            .parentCommentId(comment.getCommentId())
                                            .authorId(reply.getAuthorId())
                                            .authorName(getAuthorName(reply.getAuthorId()))
                                            .content(reply.getContent())
                                            .createdAt(reply.getCreatedAt())
                                            .likeCount(reply.getLikes() != null ? reply.getLikes().size() : 0)
                                            .build();
                                    voList.add(replyVo);
                                }
                            }
                        }
                    }
                }
            }
        }
        
        return voList;
    }

    /** 获取作者名称 */
    private String getAuthorName(Long authorId) {
        if (authorId == null) {
            return "未知用户";
        }
        try {
            String authorName = userMapper.selectNameById(authorId);
            return authorName != null ? authorName : "用户 #" + authorId;
        } catch (Exception e) {
            log.warn("获取作者用户名失败 authorId={}", authorId, e);
            return "用户 #" + authorId;
        }
    }

    /** 获取评论树（用于管理员查看评论结构，管理员用） */
    public CommentVO getCommentTree(String questionId, Long answerId, Long commentId) {
        QuestionDO q = repo.findByQuestionId(questionId);
        if (q == null) return null;

        QuestionDO.CommentDO commentDO = q.getAnswers().stream()
                .filter(a -> a.getAnswerId().equals(answerId))
                .flatMap(a -> a.getComments().stream())
                .filter(c -> c.getCommentId().equals(commentId))
                .findFirst()
                .orElse(null);

        if (commentDO == null) return null;

        return convert.toCommentVO(commentDO);
    }

    /** 统计评论下的回复数量（递归统计，管理员用） */
    public int countCommentReplies(String questionId, Long answerId, Long commentId) {
        QuestionDO q = repo.findByQuestionId(questionId);
        if (q == null) return 0;

        QuestionDO.CommentDO commentDO = q.getAnswers().stream()
                .filter(a -> a.getAnswerId().equals(answerId))
                .flatMap(a -> a.getComments().stream())
                .filter(c -> c.getCommentId().equals(commentId))
                .findFirst()
                .orElse(null);

        if (commentDO == null) return 0;

        // 统计回复数量（当前只有一层，Reply不能再有子回复）
        return commentDO.getReplies() != null ? commentDO.getReplies().size() : 0;
    }

}
