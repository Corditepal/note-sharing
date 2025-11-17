import java.util.List;

// NoteSearchService.java - 搜索服务接口
public interface NoteSearchService {
    /**
     * 根据关键词搜索笔记
     * @param keyword 搜索关键词(可以是标题或上传者名)
     * @return 匹配的笔记列表
     */
    List<Note> searchNotes(String keyword);

    /**
     * 获取随机推荐笔记
     * @param count 推荐数量
     * @return 随机推荐的笔记列表
     */
    List<Note> getRandomRecommendations(int count);
}
