// SimpleNoteRepository.java - 简单的内存数据库实现(实际项目中可替换为JDBC/JPA实现)
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
public class SimpleNoteRepository implements NoteRepository {
    private List<Note> notes = new ArrayList<>();

    public SimpleNoteRepository() {
        // 初始化一些测试数据
        notes.add(new Note("1", "数据结构笔记", "关于树和图的内容", "student1", new Date()));
        notes.add(new Note("2", "算法笔记", "动态规划算法", "student2", new Date()));
        notes.add(new Note("3", "Java编程", "面向对象编程", "student3", new Date()));
    }

    @Override
    public List<Note> findAllNotes() {
        return new ArrayList<>(notes);
    }
}
