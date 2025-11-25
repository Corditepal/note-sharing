package com.project.login.model.request.tag;

import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class TagNameRequest {

    @NotNull(message = "tagId cannot be null")
    private Long tagId;
}
