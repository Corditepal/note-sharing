package com.project.login.convert;

import com.project.login.model.dto.notespace.*;
import com.project.login.model.dataobject.NoteSpaceDO;
import com.project.login.model.request.notespace.*;
import com.project.login.model.vo.NoteSpaceVO;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.factory.Mappers;

import java.time.format.DateTimeFormatter;
import java.util.List;

@Mapper(componentModel = "spring")
public interface NoteSpaceConvert {

    NoteSpaceConvert INSTANCE = Mappers.getMapper(NoteSpaceConvert.class);

    // ------------------- Request → DTO -------------------
    NoteSpaceCreateDTO toCreateDTO(NoteSpaceCreateRequest request);

    NoteSpaceUpdateDTO toUpdateDTO(NoteSpaceUpdateRequest request);

    NoteSpaceDeleteDTO toDeleteDTO(NoteSpaceDeleteRequest request);

    NoteSpaceByUserDTO toByUserDTO(NoteSpaceListByUserRequest request);

    // ------------------- DTO → DO -------------------
    @Mapping(target = "id", ignore = true) // 新建时id数据库生成
    @Mapping(target = "userId", source = "userId")
    @Mapping(target = "tagId", ignore = true) // tagId 需在 Service 层根据 tag 名查出
    @Mapping(target = "name", source = "name")
    @Mapping(target = "createdAt", ignore = true)
    @Mapping(target = "updatedAt", ignore = true)
    NoteSpaceDO toDO(NoteSpaceCreateDTO dto);

    @Mapping(target = "id", source = "id") // 更新需要id
    @Mapping(target = "name", source = "name")
    @Mapping(target = "tagId", ignore = true)
    @Mapping(target = "userId", ignore = true)
    @Mapping(target = "createdAt", ignore = true)
    @Mapping(target = "updatedAt", ignore = true)
    NoteSpaceDO toDO(NoteSpaceUpdateDTO dto);

    // ------------------- DO → VO -------------------
    @Mapping(target = "id", source = "id")
    @Mapping(target = "name", source = "name")
    @Mapping(target = "userId", source = "userId")
    @Mapping(target = "tag", source = "tagId")
    @Mapping(target = "createTime", source = "createdAt")
    @Mapping(target = "updateTime", source = "updatedAt")
    NoteSpaceVO toVO(NoteSpaceDO doObj);

    List<NoteSpaceVO> toVOList(List<NoteSpaceDO> doList);

    // ------------------- 辅助方法 -------------------
    default String formatDateTime(java.time.LocalDateTime dateTime) {
        if (dateTime == null) return null;
        return dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }
}
