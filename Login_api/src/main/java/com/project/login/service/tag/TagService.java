package com.project.login.service.tag;

import com.project.login.convert.TagConvert;
import com.project.login.mapper.TagMapper;
import com.project.login.model.dataobject.TagDO;
import com.project.login.model.dto.tag.TagByIdDTO;
import com.project.login.model.vo.TagVO;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class TagService{

    private final TagMapper tagMapper;
    @Qualifier("tagConvert")
    private final TagConvert convert;

    @Transactional
    public TagVO getTagName(TagByIdDTO dto) {

        TagDO existing = tagMapper.selectTagById(dto.getTagId());
        if (existing == null) throw new RuntimeException("标签不存在");

        return convert.toVO(existing);
    }

    @Transactional
    public String getTagNameById(Long id) {

        TagDO existing = tagMapper.selectTagById(id);
        if (existing == null) throw new RuntimeException("标签不存在");

        return existing.getName();
    }

}
