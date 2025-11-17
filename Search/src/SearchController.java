// SearchController.java - 处理搜索请求的控制器
import java.util.List;
public class SearchController {
    private NoteSearchService searchService;

    public SearchController(NoteSearchService searchService) {
        this.searchService = searchService;
    }

    /**
     * 处理搜索请求
     * @param keyword 搜索关键词
     * @return 搜索结果响应
     */
    public SearchResponse handleSearch(String keyword) {
        List<Note> results = searchService.searchNotes(keyword);

        if (results.isEmpty()) {
            List<Note> recommendations = searchService.getRandomRecommendations(2);
            return new SearchResponse(false, "未找到匹配的笔记", recommendations);
        } else {
            return new SearchResponse(true, "找到" + results.size() + "条匹配记录", results);
        }
    }
}
