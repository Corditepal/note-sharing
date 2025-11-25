package com.project.login.convert;

import com.project.login.model.dataobject.TagDO;
import com.project.login.model.dto.tag.TagByIdDTO;
import com.project.login.model.request.tag.TagNameRequest;
import com.project.login.model.vo.TagVO;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.factory.Mappers;

import java.util.List;

@Mapper(componentModel = "spring")
public interface TagConvert {

    TagConvert INSTANCE = Mappers.getMapper(TagConvert.class);

    // ------------------- Request → DTO -------------------
    TagByIdDTO toByIdDTO(TagNameRequest request);


    // ------------------- DO → VO -------------------
    @Mapping(target = "id", source = "id")
    @Mapping(target = "tagName", source = "name")
    TagVO toVO(TagDO doObj);

    List<TagVO> toVOList(List<TagDO> doList);
}
