import java.util.List;
// NoteRepository.java - 数据访问接口
public interface NoteRepository {
    /**
     * 从数据库获取所有笔记
     * @return 所有笔记列表
     */
    List<Note> findAllNotes();

    // 其他可能的数据库操作方法...
}
