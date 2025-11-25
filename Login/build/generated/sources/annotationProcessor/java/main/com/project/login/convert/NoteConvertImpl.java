package com.project.login.convert;

import com.project.login.model.dataobject.NoteDO;
import com.project.login.model.dto.note.ImageUrlDTO;
import com.project.login.model.dto.note.NoteCreateDTO;
import com.project.login.model.dto.note.NoteDeleteDTO;
import com.project.login.model.dto.note.NoteFileUrlDTO;
import com.project.login.model.dto.note.NoteListByNotebookDTO;
import com.project.login.model.dto.note.NoteMoveDTO;
import com.project.login.model.dto.note.NoteRenameDTO;
import com.project.login.model.dto.note.NoteUpdateDTO;
import com.project.login.model.dto.note.NoteUploadFileDTO;
import com.project.login.model.request.note.ImageUrlRequest;
import com.project.login.model.request.note.NoteCreateRequest;
import com.project.login.model.request.note.NoteDeleteRequest;
import com.project.login.model.request.note.NoteFileUrlRequest;
import com.project.login.model.request.note.NoteListByNotebookRequest;
import com.project.login.model.request.note.NoteMoveRequest;
import com.project.login.model.request.note.NoteRenameRequest;
import com.project.login.model.request.note.NoteUpdateRequest;
import com.project.login.model.request.note.NoteUploadFileRequest;
import com.project.login.model.vo.NoteVO;
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
public class NoteConvertImpl implements NoteConvert {

    @Override
    public NoteCreateDTO toCreateDTO(NoteCreateRequest request) {
        if ( request == null ) {
            return null;
        }

        NoteCreateDTO noteCreateDTO = new NoteCreateDTO();

        noteCreateDTO.setMeta( request.getMeta() );
        noteCreateDTO.setFile( request.getFile() );

        return noteCreateDTO;
    }

    @Override
    public NoteUpdateDTO toUpdateDTO(NoteUpdateRequest request) {
        if ( request == null ) {
            return null;
        }

        NoteUpdateDTO noteUpdateDTO = new NoteUpdateDTO();

        noteUpdateDTO.setMeta( request.getMeta() );
        noteUpdateDTO.setFile( request.getFile() );

        return noteUpdateDTO;
    }

    @Override
    public NoteDeleteDTO toDeleteDTO(NoteDeleteRequest request) {
        if ( request == null ) {
            return null;
        }

        NoteDeleteDTO noteDeleteDTO = new NoteDeleteDTO();

        noteDeleteDTO.setNoteId( request.getNoteId() );

        return noteDeleteDTO;
    }

    @Override
    public NoteMoveDTO toMoveDTO(NoteMoveRequest request) {
        if ( request == null ) {
            return null;
        }

        NoteMoveDTO noteMoveDTO = new NoteMoveDTO();

        noteMoveDTO.setNoteId( request.getNoteId() );
        noteMoveDTO.setTargetNotebookId( request.getTargetNotebookId() );

        return noteMoveDTO;
    }

    @Override
    public NoteListByNotebookDTO toListByNotebookDTO(NoteListByNotebookRequest request) {
        if ( request == null ) {
            return null;
        }

        NoteListByNotebookDTO noteListByNotebookDTO = new NoteListByNotebookDTO();

        noteListByNotebookDTO.setNotebookID( request.getNotebookID() );

        return noteListByNotebookDTO;
    }

    @Override
    public NoteUploadFileDTO toUploadFileDTO(NoteUploadFileRequest request) {
        if ( request == null ) {
            return null;
        }

        NoteUploadFileDTO noteUploadFileDTO = new NoteUploadFileDTO();

        noteUploadFileDTO.setMeta( request.getMeta() );
        noteUploadFileDTO.setFile( request.getFile() );

        return noteUploadFileDTO;
    }

    @Override
    public NoteFileUrlDTO toFileUrlDTO(NoteFileUrlRequest request) {
        if ( request == null ) {
            return null;
        }

        NoteFileUrlDTO noteFileUrlDTO = new NoteFileUrlDTO();

        noteFileUrlDTO.setFilename( request.getFilename() );

        return noteFileUrlDTO;
    }

    @Override
    public ImageUrlDTO toImageUrlDTO(ImageUrlRequest request) {
        if ( request == null ) {
            return null;
        }

        ImageUrlDTO imageUrlDTO = new ImageUrlDTO();

        imageUrlDTO.setFile( request.getFile() );

        return imageUrlDTO;
    }

    @Override
    public NoteRenameDTO toRenameDTO(NoteRenameRequest request) {
        if ( request == null ) {
            return null;
        }

        NoteRenameDTO noteRenameDTO = new NoteRenameDTO();

        noteRenameDTO.setId( request.getId() );
        noteRenameDTO.setNewName( request.getNewName() );

        return noteRenameDTO;
    }

    @Override
    public NoteVO toVO(NoteDO noteDO) {
        if ( noteDO == null ) {
            return null;
        }

        NoteVO noteVO = new NoteVO();

        noteVO.setId( noteDO.getId() );
        noteVO.setTitle( noteDO.getTitle() );
        noteVO.setFilename( noteDO.getFilename() );
        noteVO.setFileType( noteDO.getFileType() );
        noteVO.setNotebookId( noteDO.getNotebookId() );
        noteVO.setCreatedAt( noteDO.getCreatedAt() );
        noteVO.setUpdatedAt( noteDO.getUpdatedAt() );

        return noteVO;
    }

    @Override
    public List<NoteVO> toVOList(List<NoteDO> doList) {
        if ( doList == null ) {
            return null;
        }

        List<NoteVO> list = new ArrayList<NoteVO>( doList.size() );
        for ( NoteDO noteDO : doList ) {
            list.add( toVO( noteDO ) );
        }

        return list;
    }
}
