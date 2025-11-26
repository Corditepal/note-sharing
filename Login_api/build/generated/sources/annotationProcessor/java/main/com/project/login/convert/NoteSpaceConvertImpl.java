package com.project.login.convert;

import com.project.login.model.dataobject.NoteSpaceDO;
import com.project.login.model.dto.notespace.NoteSpaceByUserDTO;
import com.project.login.model.dto.notespace.NoteSpaceCreateDTO;
import com.project.login.model.dto.notespace.NoteSpaceDeleteDTO;
import com.project.login.model.dto.notespace.NoteSpaceUpdateDTO;
import com.project.login.model.request.notespace.NoteSpaceCreateRequest;
import com.project.login.model.request.notespace.NoteSpaceDeleteRequest;
import com.project.login.model.request.notespace.NoteSpaceListByUserRequest;
import com.project.login.model.request.notespace.NoteSpaceUpdateRequest;
import com.project.login.model.vo.NoteSpaceVO;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.processing.Generated;
import org.springframework.stereotype.Component;

@Generated(
    value = "org.mapstruct.ap.MappingProcessor",
    date = "2025-11-25T15:25:14+0800",
    comments = "version: 1.6.3, compiler: IncrementalProcessingEnvironment from gradle-language-java-9.2.0.jar, environment: Java 25.0.1 (Oracle Corporation)"
)
@Component
public class NoteSpaceConvertImpl implements NoteSpaceConvert {

    @Override
    public NoteSpaceCreateDTO toCreateDTO(NoteSpaceCreateRequest request) {
        if ( request == null ) {
            return null;
        }

        NoteSpaceCreateDTO noteSpaceCreateDTO = new NoteSpaceCreateDTO();

        noteSpaceCreateDTO.setName( request.getName() );
        noteSpaceCreateDTO.setUserId( request.getUserId() );
        noteSpaceCreateDTO.setTag( request.getTag() );

        return noteSpaceCreateDTO;
    }

    @Override
    public NoteSpaceUpdateDTO toUpdateDTO(NoteSpaceUpdateRequest request) {
        if ( request == null ) {
            return null;
        }

        NoteSpaceUpdateDTO noteSpaceUpdateDTO = new NoteSpaceUpdateDTO();

        noteSpaceUpdateDTO.setId( request.getId() );
        noteSpaceUpdateDTO.setName( request.getName() );
        noteSpaceUpdateDTO.setTag( request.getTag() );

        return noteSpaceUpdateDTO;
    }

    @Override
    public NoteSpaceDeleteDTO toDeleteDTO(NoteSpaceDeleteRequest request) {
        if ( request == null ) {
            return null;
        }

        NoteSpaceDeleteDTO noteSpaceDeleteDTO = new NoteSpaceDeleteDTO();

        noteSpaceDeleteDTO.setId( request.getId() );

        return noteSpaceDeleteDTO;
    }

    @Override
    public NoteSpaceByUserDTO toByUserDTO(NoteSpaceListByUserRequest request) {
        if ( request == null ) {
            return null;
        }

        NoteSpaceByUserDTO noteSpaceByUserDTO = new NoteSpaceByUserDTO();

        noteSpaceByUserDTO.setUserId( request.getUserId() );

        return noteSpaceByUserDTO;
    }

    @Override
    public NoteSpaceDO toDO(NoteSpaceCreateDTO dto) {
        if ( dto == null ) {
            return null;
        }

        NoteSpaceDO.NoteSpaceDOBuilder noteSpaceDO = NoteSpaceDO.builder();

        noteSpaceDO.userId( dto.getUserId() );
        noteSpaceDO.name( dto.getName() );

        return noteSpaceDO.build();
    }

    @Override
    public NoteSpaceDO toDO(NoteSpaceUpdateDTO dto) {
        if ( dto == null ) {
            return null;
        }

        NoteSpaceDO.NoteSpaceDOBuilder noteSpaceDO = NoteSpaceDO.builder();

        noteSpaceDO.id( dto.getId() );
        noteSpaceDO.name( dto.getName() );

        return noteSpaceDO.build();
    }

    @Override
    public NoteSpaceVO toVO(NoteSpaceDO doObj) {
        if ( doObj == null ) {
            return null;
        }

        NoteSpaceVO noteSpaceVO = new NoteSpaceVO();

        noteSpaceVO.setId( doObj.getId() );
        noteSpaceVO.setName( doObj.getName() );
        noteSpaceVO.setUserId( doObj.getUserId() );
        noteSpaceVO.setTag( doObj.getTagId() );
        noteSpaceVO.setCreateTime( formatDateTime( doObj.getCreatedAt() ) );
        noteSpaceVO.setUpdateTime( formatDateTime( doObj.getUpdatedAt() ) );

        return noteSpaceVO;
    }

    @Override
    public List<NoteSpaceVO> toVOList(List<NoteSpaceDO> doList) {
        if ( doList == null ) {
            return null;
        }

        List<NoteSpaceVO> list = new ArrayList<NoteSpaceVO>( doList.size() );
        for ( NoteSpaceDO noteSpaceDO : doList ) {
            list.add( toVO( noteSpaceDO ) );
        }

        return list;
    }
}
