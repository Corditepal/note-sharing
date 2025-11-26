package com.project.login.service.admin;

import com.project.login.model.request.admin.UserAdminUpdateRequest;
import com.project.login.model.entity.UserEntity;
import com.project.login.mapper.NoteSpaceMapper;
import com.project.login.mapper.UserMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Objects;

@Service
@RequiredArgsConstructor
public class AdminUserService {

    private final UserMapper userMapper;
    private final NoteSpaceMapper noteSpaceMapper;

    @Transactional
    public UserEntity updateUser(Long id, UserAdminUpdateRequest req) {
        UserEntity user = userMapper.selectById(id);
        if (user == null) {
            throw new IllegalArgumentException("用户不存在");
        }

        if (req.getUsername() != null && !Objects.equals(user.getUsername(), req.getUsername())) {
            user.setUsername(req.getUsername());
        }

        if (req.getStudentNumber() != null && !Objects.equals(user.getStudentNumber(), req.getStudentNumber())) {
            UserEntity snUser = userMapper.selectByStudentNumber(req.getStudentNumber());
            if (snUser != null && !Objects.equals(snUser.getId(), id)) {
                throw new IllegalArgumentException("学号已存在");
            }
            user.setStudentNumber(req.getStudentNumber());
        }

        if (req.getEmail() != null && !Objects.equals(user.getEmail(), req.getEmail())) {
            UserEntity mailUser = userMapper.selectByEmail(req.getEmail());
            if (mailUser != null && !Objects.equals(mailUser.getId(), id)) {
                throw new IllegalArgumentException("邮箱已存在");
            }
            user.setEmail(req.getEmail());
        }

        userMapper.updateUser(user);
        return userMapper.selectById(id);
    }

    @Transactional
    public void deleteUser(Long id) {
        UserEntity user = userMapper.selectById(id);
        if (user == null) {
            throw new IllegalArgumentException("用户不存在");
        }
        noteSpaceMapper.selectByUser(id).forEach(space -> noteSpaceMapper.deleteNoteSpace(space.getId()));
        userMapper.deleteUserById(id);
    }
}
