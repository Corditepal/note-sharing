package com.project.login.mapper;

import com.project.login.model.dataobject.UserDO;
import com.project.login.model.entity.UserEntity;
import org.apache.ibatis.annotations.*;

@Mapper
public interface UserMapper {

    @Select("SELECT id, username, password_hash AS passwordHash, enabled, studentNumber, email, " +
            "created_at AS createdAt, updated_at AS updatedAt, avatar_url AS avatarUrl " +
            "FROM users WHERE id = #{id}")
    UserDO selectById(@Param("id") Long id);

    @Select("SELECT username " +
            "FROM users WHERE id = #{id}")
    String selectNameById(Long id);

    @Select("SELECT id, username, password_hash AS passwordHash, enabled, studentNumber, email, " +
            "created_at AS createdAt, updated_at AS updatedAt, avatar_url AS avatarUrl " +
            "FROM users WHERE email = #{email}")
    UserEntity selectByEmail(@Param("email") String email);

    @Select("SELECT id, username, password_hash AS passwordHash, enabled, studentNumber, email, " +
            "created_at AS createdAt, updated_at AS updatedAt, avatar_url AS avatarUrl " +
            "FROM users WHERE username = #{username}")
    UserEntity selectByUsername(@Param("username") String username);

    @Insert("INSERT INTO users (username, password_hash, enabled, studentNumber, email, created_at, updated_at) " +
            "VALUES (#{username}, #{passwordHash}, #{enabled}, #{studentNumber}, #{email}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)")
    @Options(useGeneratedKeys = true, keyProperty = "id")
    void insertUser(UserEntity user);

    @Update("UPDATE users " +
            "SET username = #{username}, " +
            "    password_hash = #{passwordHash}, " +
            "    enabled = #{enabled}, " +
            "    studentNumber = #{studentNumber}, " +
            "    email = #{email}, " +
            "    avatar_url = #{avatarUrl}, " +
            "    updated_at = CURRENT_TIMESTAMP " +
            "WHERE id = #{id}")
    void updateUser(UserEntity user);

    @Update("UPDATE users " +
            "SET avatar_url = #{avatarUrl}, " +
            "    updated_at = CURRENT_TIMESTAMP " +
            "WHERE id = #{userId}")
    void updateAvatarUrl(@Param("userId") Long userId, @Param("avatarUrl") String avatarUrl);
}