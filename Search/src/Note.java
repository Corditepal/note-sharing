import java.util.Date;

// Note.java - 笔记实体类
public class Note {
    private String noteId;
    private String title;
    private String content;
    private String uploaderId;


    // 构造函数、getter和setter方法
    public Note(String noteId, String title, String content, String uploaderId, Date uploadDate) {
        this.noteId = noteId;
        this.title = title;
        this.content = content;
        this.uploaderId = uploaderId;
    }

    // getter方法略...
    // Getter 方法实现
    public String getId() {
        return noteId;
    }

    public String getTitle() {
        return title;
    }

    public String getUploaderId() {
        return uploaderId;
    }

    public String getContent() {
        return content;
    }
}
