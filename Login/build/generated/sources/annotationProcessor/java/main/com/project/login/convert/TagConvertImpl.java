package com.project.login.convert;

import com.project.login.model.dataobject.TagDO;
import com.project.login.model.dto.tag.TagByIdDTO;
import com.project.login.model.request.tag.TagNameRequest;
import com.project.login.model.vo.TagVO;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.processing.Generated;
import org.springframework.stereotype.Component;

@Generated(
    value = "org.mapstruct.ap.MappingProcessor",
    date = "2025-11-25T15:25:13+0800",
    comments = "version: 1.6.3, compiler: IncrementalProcessingEnvironment from gradle-language-java-9.2.0.jar, environment: Java 25.0.1 (Oracle Corporation)"
)
@Component
public class TagConvertImpl implements TagConvert {

    @Override
    public TagByIdDTO toByIdDTO(TagNameRequest request) {
        if ( request == null ) {
            return null;
        }

        TagByIdDTO tagByIdDTO = new TagByIdDTO();

        tagByIdDTO.setTagId( request.getTagId() );

        return tagByIdDTO;
    }

    @Override
    public TagVO toVO(TagDO doObj) {
        if ( doObj == null ) {
            return null;
        }

        TagVO tagVO = new TagVO();

        tagVO.setId( doObj.getId() );
        tagVO.setTagName( doObj.getName() );

        return tagVO;
    }

    @Override
    public List<TagVO> toVOList(List<TagDO> doList) {
        if ( doList == null ) {
            return null;
        }

        List<TagVO> list = new ArrayList<TagVO>( doList.size() );
        for ( TagDO tagDO : doList ) {
            list.add( toVO( tagDO ) );
        }

        return list;
    }
}
