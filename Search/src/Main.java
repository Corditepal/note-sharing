// Main.java - 程序入口
public class Main {
    public static void main(String[] args) {
        // 初始化依赖
        NoteRepository repository = new SimpleNoteRepository();
        NoteSearchService searchService = new NoteSearchServiceImpl(repository);
        SearchController controller = new SearchController(searchService);

        // 模拟搜索请求
        System.out.println("搜索'算法':");
        SearchResponse response1 = controller.handleSearch("算法");
        printResponse(response1);

        System.out.println("\n搜索'不存在的内容':");
        SearchResponse response2 = controller.handleSearch("不存在的内容");
        printResponse(response2);
    }

    private static void printResponse(SearchResponse response) {
        System.out.println(response.getMessage());
        for (Note note : response.getNotes()) {
            System.out.println(" - " + note.getTitle() + " (上传者: " + note.getUploaderId() + " 内容: "+note.getContent()+")");
        }
    }
}