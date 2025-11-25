package com.project.login.model.dto.note;

import com.project.login.model.request.note.NoteMeta;
import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.springframework.web.multipart.MultipartFile;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class NoteCreateDTO {
    private NoteMeta meta;
    private MultipartFile file;
}