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

    /** 删除评论 */
    public void deleteComment(String questionId, Long answerId, Long commentId) {
        QuestionDO q = repo.findByQuestionId(questionId);
        if (q == null) return;

        q.getAnswers().stream()
                .filter(a -> a.getAnswerId().equals(answerId))
                .findFirst()
                .ifPresent(a ->
                        a.getComments().removeIf(c -> c.getCommentId().equals(commentId))
                );

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

}
