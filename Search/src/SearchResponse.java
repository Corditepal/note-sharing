// SearchResponse.java - 搜索响应对象
import java.util.List;
public class SearchResponse {
    private boolean success;
    private String message;
    private List<Note> notes;

    public SearchResponse(boolean success, String message, List<Note> notes) {
        this.success = success;
        this.message = message;
        this.notes = notes;
    }

    // getter方法略...
    public List<Note> getNotes() {
        return notes;
    }
    public String getMessage() {
        return message;
    }
}
