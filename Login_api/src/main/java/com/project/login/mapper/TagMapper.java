package com.project.login.mapper;

import com.project.login.model.dataobject.TagDO;
import org.apache.ibatis.annotations.*;

@Mapper
public interface TagMapper {

    @Select("SELECT id FROM tags WHERE name = #{name}")
    Long selectIdByName(@Param("name") String name);

    @Select("SELECT id, name FROM tags WHERE id = #{id}")
    @Results({
            @Result(column = "id", property = "id"),
            @Result(column = "name", property = "name")
    })
    TagDO selectTagById(@Param("id") Long id);

    @Select("SELECT id, name FROM tags WHERE name = #{name}")
    @Results({
            @Result(column = "id", property = "id"),
            @Result(column = "name", property = "name")
    })
    TagDO selectTagByName(@Param("name") String name);

    @Insert("INSERT INTO tags (name) VALUES (#{name})")
    @Options(useGeneratedKeys = true, keyProperty = "id")
    void insertTag(TagDO tag);

    @Delete("DELETE FROM tags WHERE id = #{id}")
    void deleteById(Long id);
}
