package com.project.login.model.dto.note;

import com.project.login.model.request.note.NoteUpdateMeta;
import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.springframework.web.multipart.MultipartFile;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class NoteUpdateDTO {
    private NoteUpdateMeta meta;
    private MultipartFile file;
}