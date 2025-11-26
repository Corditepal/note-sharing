package com.project.login.mapper;

import com.project.login.model.response.admin.CommentAdminVO;
import org.apache.ibatis.annotations.*;

import java.time.LocalDateTime;
import java.util.List;

@Mapper
public interface CommentMapper {

    @Select({
            "<script>",
            "SELECT c.id AS id,",
            "       c.content AS content,",
            "       u.username AS username,",
            "       n.title AS note,",
            "       c.parent_id AS parentId,",
            "       c.likes AS likes,",
            "       c.replies AS replies,",
            "       DATE_FORMAT(c.created_at, '%Y-%m-%d') AS createdAt",
            "FROM comments c",
            "JOIN users u ON u.id = c.user_id",
            "JOIN notes n ON n.id = c.note_id",
            "<where>",
            "  <if test='publisher != null and publisher != \"\"'>",
            "    AND u.username LIKE CONCAT('%', #{publisher}, '%')",
            "  </if>",
            "  <if test='start != null'>",
            "    AND c.created_at &gt;= #{start}",
            "  </if>",
            "  <if test='end != null'>",
            "    AND c.created_at &lt; #{end}",
            "  </if>",
            "</where>",
            "ORDER BY c.created_at DESC",
            "</script>"
    })
    List<CommentAdminVO> search(@Param("publisher") String publisher,
                                @Param("start") LocalDateTime start,
                                @Param("end") LocalDateTime end);

    @Delete("DELETE FROM comments WHERE id = #{id}")
    int deleteById(@Param("id") Long id);
}

