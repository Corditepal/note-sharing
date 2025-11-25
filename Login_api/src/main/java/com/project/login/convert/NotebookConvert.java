package com.project.login.convert;

import com.project.login.model.dataobject.NotebookDO;
import com.project.login.model.dto.notebook.*;
import com.project.login.model.request.notebook.*;
import com.project.login.model.vo.NotebookVO;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.factory.Mappers;

import java.util.List;

@Mapper(componentModel = "spring")
public interface NotebookConvert {

    NotebookConvert INSTANCE = Mappers.getMapper(NotebookConvert.class);

    // --- Request -> DTO 转换 (用于 Controller 到 Service) ---
    NotebookCreateDTO toCreateDTO(NotebookCreateRequest request);
    NotebookUpdateDTO toUpdateDTO(NotebookUpdateRequest request);
    NotebookDeleteDTO toDeleteDTO(NotebookDeleteRequest request);
    NotebookMoveDTO toMoveDTO(NotebookMoveRequest request);
    NotebookBySpaceDTO toBySpaceDTO(NotebookListBySpaceRequest request);


    // --- DO -> VO 转换 (用于 Service 到 Controller 响应) ---
    List<NotebookVO> toVOList(List<NotebookDO> doList);
    NotebookVO toVO(NotebookDO notebookDO);

    // --- DTO -> DO 转换 (用于 Service 到 Mybatis 数据库持久化) ---
    @Mapping(target = "id", ignore = true)
    @Mapping(target = "createdAt", ignore = true)
    @Mapping(target = "updatedAt", ignore = true)
    @Mapping(target = "tagId", ignore = true)
    @Mapping(target = "notes", ignore = true)
    NotebookDO toDO(NotebookCreateDTO dto);

    @Mapping(target = "spaceId", ignore = true)
    @Mapping(target = "createdAt", ignore = true)
    @Mapping(target = "updatedAt", ignore = true)
    @Mapping(target = "tagId", ignore = true)
    @Mapping(target = "notes", ignore = true)
    NotebookDO toDO(NotebookUpdateDTO dto);
}