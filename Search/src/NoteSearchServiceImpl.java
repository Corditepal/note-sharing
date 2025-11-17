// NoteSearchServiceImpl.java - 搜索服务实现
import java.util.List;
import java.util.ArrayList;
import java.util.Random;

public class NoteSearchServiceImpl implements NoteSearchService {
    private NoteRepository noteRepository;

    public NoteSearchServiceImpl(NoteRepository noteRepository) {
        this.noteRepository = noteRepository;
    }

    @Override
    public List<Note> searchNotes(String keyword) {
        // 从数据库获取所有笔记
        List<Note> allNotes = noteRepository.findAllNotes();
        List<Note> matchedNotes = new ArrayList<>();

        // 简单的线性搜索实现
        for (Note note : allNotes) {
            if (note.getTitle().contains(keyword) || note.getUploaderId().contains(keyword)) {
                matchedNotes.add(note);
            }
        }

        return matchedNotes;
    }

    @Override
    public List<Note> getRandomRecommendations(int count) {
        List<Note> allNotes = noteRepository.findAllNotes();
        List<Note> recommendations = new ArrayList<>();

        if (allNotes.isEmpty()) {
            return recommendations;
        }

        Random random = new Random();
        for (int i = 0; i < count && i < allNotes.size(); i++) {
            int randomIndex = random.nextInt(allNotes.size());
            recommendations.add(allNotes.get(randomIndex));
        }

        return recommendations;
    }
}
