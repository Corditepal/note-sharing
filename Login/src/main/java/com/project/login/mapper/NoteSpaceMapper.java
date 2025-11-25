package com.project.login.mapper;

import com.project.login.model.dataobject.NoteSpaceDO;
import org.apache.ibatis.annotations.*;

import java.util.List;

@Mapper
public interface NoteSpaceMapper {

    @Insert("INSERT INTO note_spaces (name, user_id, tag_id, created_at, updated_at) " +
            "VALUES (#{name}, #{userId}, #{tagId}, #{createdAt}, #{updatedAt})")
    @Options(useGeneratedKeys = true, keyProperty = "id")
    void insertNoteSpace(NoteSpaceDO noteSpaceDO);

    @Update("UPDATE note_spaces SET name = #{name}, tag_id = #{tagId}, updated_at = #{updatedAt} WHERE id = #{id}")
    void updateNoteSpace(NoteSpaceDO noteSpaceDO);

    @Delete("DELETE FROM note_spaces WHERE id = #{id}")
    void deleteNoteSpace(Long id);

    @Select("""
            SELECT ns.id, ns.name, ns.user_id, ns.tag_id,
                   ns.created_at, ns.updated_at
            FROM note_spaces ns
            WHERE ns.user_id = #{userId}
            """)
    @Results({
            @Result(column = "id", property = "id"),
            @Result(column = "name", property = "name"),
            @Result(column = "user_id", property = "userId"),
            @Result(column = "tag_id", property = "tagId"),
            @Result(column = "created_at", property = "createdAt"),
            @Result(column = "updated_at", property = "updatedAt")
    })
    List<NoteSpaceDO> selectByUser(Long userId);

    @Select("""
            SELECT ns.id, ns.name, ns.user_id, ns.tag_id,
                   ns.created_at, ns.updated_at
            FROM note_spaces ns
            WHERE ns.id = #{id}
            """)
    @Results({
            @Result(column = "id", property = "id"),
            @Result(column = "name", property = "name"),
            @Result(column = "user_id", property = "userId"),
            @Result(column = "tag_id", property = "tagId"),
            @Result(column = "created_at", property = "createdAt"),
            @Result(column = "updated_at", property = "updatedAt")
    })
    NoteSpaceDO selectById(Long id);

    @Select("SELECT COUNT(*) FROM note_spaces WHERE tag_id = #{tagId}")
    Integer countByTagId(Long tagId);
}
