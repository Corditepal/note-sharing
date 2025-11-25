package com.project.login.mapper;

import com.project.login.model.dataobject.NotebookDO;
import org.apache.ibatis.annotations.*;

import java.util.List;

@Mapper
public interface NotebookMapper {

    @Insert("INSERT INTO notebooks (name, space_id, tag_id, created_at, updated_at) " +
            "VALUES (#{name}, #{spaceId}, #{tagId}, #{createdAt}, #{updatedAt})")
    @Options(useGeneratedKeys = true, keyProperty = "id")
    void insert(NotebookDO notebook);

    @Select("SELECT id, name, space_id, tag_id, created_at, updated_at FROM notebooks WHERE id = #{id}")
    @Results(id = "NotebookBaseResultMap", value = {
            @Result(column = "id", property = "id", id = true),
            @Result(column = "name", property = "name"),
            @Result(column = "space_id", property = "spaceId"),
            @Result(column = "tag_id", property = "tagId"),
            @Result(column = "created_at", property = "createdAt"),
            @Result(column = "updated_at", property = "updatedAt")
    })
    NotebookDO selectById(Long id);

    @Select("SELECT id, name, space_id, tag_id, created_at, updated_at " +
            "FROM notebooks WHERE space_id = #{spaceId}")
    @ResultMap("NotebookBaseResultMap")
    List<NotebookDO> selectBySpaceId(Long spaceId);

    @Update("""
            UPDATE notebooks
            SET name = #{name},
                space_id = #{spaceId},
                tag_id = #{tagId},
                updated_at = #{updatedAt}
            WHERE id = #{id}
            """)
    void update(NotebookDO notebook);

    @Delete("DELETE FROM notebooks WHERE id = #{id}")
    void deleteById(Long id);

    @Select("SELECT COUNT(*) FROM notebooks WHERE tag_id = #{tagId}")
    Integer countByTagId(Long tagId);
}
