package com.project.login.model.entity;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.time.LocalDateTime;
import java.util.List;

@Data
@Document(indexName = "notes")
public class NoteEntity {

    @Id
    private Long id; // MySQL note.id

    @Field(type = FieldType.Text)
    private String title;

    @Field(type = FieldType.Text)
    private String contentSummary; // 对应 content_summary

    @Field(type = FieldType.Keyword)
    private List<String> tags; // 笔记空间/笔记本标签

    @Field(type = FieldType.Keyword)
    private String authorName;

    @Field(type = FieldType.Integer)
    private Integer viewCount;

    @Field(type = FieldType.Integer)
    private Integer likeCount;

    @Field(type = FieldType.Integer)
    private Integer favoriteCount;

    @Field(type = FieldType.Integer)
    private Integer commentCount;

    @Field(type = FieldType.Date)
    private LocalDateTime updatedAt;

}
