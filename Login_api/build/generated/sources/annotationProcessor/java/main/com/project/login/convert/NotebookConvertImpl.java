package com.project.login.convert;

import com.project.login.model.dataobject.NotebookDO;
import com.project.login.model.dto.notebook.NotebookBySpaceDTO;
import com.project.login.model.dto.notebook.NotebookCreateDTO;
import com.project.login.model.dto.notebook.NotebookDeleteDTO;
import com.project.login.model.dto.notebook.NotebookMoveDTO;
import com.project.login.model.dto.notebook.NotebookUpdateDTO;
import com.project.login.model.request.notebook.NotebookCreateRequest;
import com.project.login.model.request.notebook.NotebookDeleteRequest;
import com.project.login.model.request.notebook.NotebookListBySpaceRequest;
import com.project.login.model.request.notebook.NotebookMoveRequest;
import com.project.login.model.request.notebook.NotebookUpdateRequest;
import com.project.login.model.vo.NotebookVO;
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
public class NotebookConvertImpl implements NotebookConvert {

    @Override
    public NotebookCreateDTO toCreateDTO(NotebookCreateRequest request) {
        if ( request == null ) {
            return null;
        }

        NotebookCreateDTO.NotebookCreateDTOBuilder notebookCreateDTO = NotebookCreateDTO.builder();

        notebookCreateDTO.name( request.getName() );
        notebookCreateDTO.spaceId( request.getSpaceId() );
        notebookCreateDTO.tag( request.getTag() );

        return notebookCreateDTO.build();
    }

    @Override
    public NotebookUpdateDTO toUpdateDTO(NotebookUpdateRequest request) {
        if ( request == null ) {
            return null;
        }

        NotebookUpdateDTO.NotebookUpdateDTOBuilder notebookUpdateDTO = NotebookUpdateDTO.builder();

        notebookUpdateDTO.id( request.getId() );
        notebookUpdateDTO.name( request.getName() );
        notebookUpdateDTO.tag( request.getTag() );

        return notebookUpdateDTO.build();
    }

    @Override
    public NotebookDeleteDTO toDeleteDTO(NotebookDeleteRequest request) {
        if ( request == null ) {
            return null;
        }

        NotebookDeleteDTO.NotebookDeleteDTOBuilder notebookDeleteDTO = NotebookDeleteDTO.builder();

        notebookDeleteDTO.id( request.getId() );

        return notebookDeleteDTO.build();
    }

    @Override
    public NotebookMoveDTO toMoveDTO(NotebookMoveRequest request) {
        if ( request == null ) {
            return null;
        }

        NotebookMoveDTO.NotebookMoveDTOBuilder notebookMoveDTO = NotebookMoveDTO.builder();

        notebookMoveDTO.targetNoteSpaceId( request.getTargetNoteSpaceId() );
        notebookMoveDTO.notebookId( request.getNotebookId() );

        return notebookMoveDTO.build();
    }

    @Override
    public NotebookBySpaceDTO toBySpaceDTO(NotebookListBySpaceRequest request) {
        if ( request == null ) {
            return null;
        }

        NotebookBySpaceDTO.NotebookBySpaceDTOBuilder notebookBySpaceDTO = NotebookBySpaceDTO.builder();

        notebookBySpaceDTO.spaceId( request.getSpaceId() );

        return notebookBySpaceDTO.build();
    }

    @Override
    public List<NotebookVO> toVOList(List<NotebookDO> doList) {
        if ( doList == null ) {
            return null;
        }

        List<NotebookVO> list = new ArrayList<NotebookVO>( doList.size() );
        for ( NotebookDO notebookDO : doList ) {
            list.add( toVO( notebookDO ) );
        }

        return list;
    }

    @Override
    public NotebookVO toVO(NotebookDO notebookDO) {
        if ( notebookDO == null ) {
            return null;
        }

        NotebookVO notebookVO = new NotebookVO();

        notebookVO.setId( notebookDO.getId() );
        notebookVO.setName( notebookDO.getName() );
        notebookVO.setSpaceId( notebookDO.getSpaceId() );
        notebookVO.setTagId( notebookDO.getTagId() );
        notebookVO.setCreatedAt( notebookDO.getCreatedAt() );
        notebookVO.setUpdatedAt( notebookDO.getUpdatedAt() );

        return notebookVO;
    }

    @Override
    public NotebookDO toDO(NotebookCreateDTO dto) {
        if ( dto == null ) {
            return null;
        }

        NotebookDO.NotebookDOBuilder notebookDO = NotebookDO.builder();

        notebookDO.name( dto.getName() );
        notebookDO.spaceId( dto.getSpaceId() );

        return notebookDO.build();
    }

    @Override
    public NotebookDO toDO(NotebookUpdateDTO dto) {
        if ( dto == null ) {
            return null;
        }

        NotebookDO.NotebookDOBuilder notebookDO = NotebookDO.builder();

        notebookDO.id( dto.getId() );
        notebookDO.name( dto.getName() );

        return notebookDO.build();
    }
}
