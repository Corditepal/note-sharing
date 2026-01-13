package com.project.login.model.vo.qa;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class QACommentDetailVO {
    /** 评论/回复ID */
    private Long id;
    
    /** 类型：COMMENT（一级评论）或 REPLY（二级回复） */
    private String type;
    
    /** 所属问题ID */
    private String questionId;
    
    /** 问题标题 */
    private String questionTitle;
    
    /** 所属回答ID */
    private Long answerId;
    
    /** 父评论ID（如果是回复，指向评论ID） */
    private Long parentCommentId;
    
    /** 作者ID */
    private Long authorId;
    
    /** 作者名称 */
    private String authorName;
    
    /** 内容 */
    private String content;
    
    /** 创建时间 */
    private LocalDateTime createdAt;
    
    /** 点赞数 */
    private Integer likeCount;
}
