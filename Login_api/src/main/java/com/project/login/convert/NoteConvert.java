package com.project.login.convert;

import com.project.login.model.dataobject.NoteDO; // 假设存在 NoteDO
import com.project.login.model.dto.note.*;
import com.project.login.model.request.note.*;
import com.project.login.model.vo.NoteVO;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.factory.Mappers;

import java.util.List;

@Mapper(componentModel = "spring")
public interface NoteConvert {

    NoteConvert INSTANCE = Mappers.getMapper(NoteConvert.class);

    // --- 1. Request -> DTO 转换 (用于 Controller 到 Service) -------------------
    NoteCreateDTO toCreateDTO(NoteCreateRequest request);
    NoteUpdateDTO toUpdateDTO(NoteUpdateRequest request);
    NoteDeleteDTO toDeleteDTO(NoteDeleteRequest request);
    NoteMoveDTO toMoveDTO(NoteMoveRequest request);
    NoteListByNotebookDTO toListByNotebookDTO(NoteListByNotebookRequest request);
    NoteUploadFileDTO toUploadFileDTO(NoteUploadFileRequest request);
    NoteFileUrlDTO toFileUrlDTO(NoteFileUrlRequest request);
    ImageUrlDTO toImageUrlDTO(ImageUrlRequest request);
    NoteRenameDTO toRenameDTO(NoteRenameRequest request);


    // --- 2. DO/Entity -> VO 转换 (用于 Service 到 Controller 响应) -----------------
    NoteVO toVO(NoteDO noteDO);
    List<NoteVO> toVOList(List<NoteDO> doList);

}