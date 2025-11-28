package com.project.login.service.notestats;

import com.project.login.mapper.NoteStatsCompensationMapper;
import com.project.login.mapper.NoteStatsMapper;
import com.project.login.model.dataobject.NoteStatsCompensationDO;
import com.project.login.model.dataobject.NoteStatsDO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class CompensationService {

    private final NoteStatsCompensationMapper compensationMapper;
    private final NoteStatsMapper noteStatsMapper;
    private static final int MAX_RETRY = 3;

    public void retryCompensation() {
        List<NoteStatsCompensationDO> pendingList = compensationMapper.getPendingOrFailed();
        for (NoteStatsCompensationDO comp : pendingList) {
            try {
                // 重新读取 DB，计算 delta = comp.total - db.total
                NoteStatsDO db = noteStatsMapper.getById(comp.getNoteId());
                long dv = comp.getViews() - (db == null || db.getViews() == null ? 0L : db.getViews());
                long dl = comp.getLikes() - (db == null || db.getLikes() == null ? 0L : db.getLikes());
                long df = comp.getFavorites() - (db == null || db.getFavorites() == null ? 0L : db.getFavorites());
                long dc = comp.getComments() - (db == null || db.getComments() == null ? 0L : db.getComments());

                // 构造 deltas，只取正数（如果为负则忽略）
                NoteStatsDO deltas = new NoteStatsDO();
                deltas.setNoteId(comp.getNoteId());
                deltas.setViews(Math.max(0, dv));
                deltas.setLikes(Math.max(0, dl));
                deltas.setFavorites(Math.max(0, df));
                deltas.setComments(Math.max(0, dc));
                deltas.setLastActivityAt(comp.getLastActivityAt() != null ? comp.getLastActivityAt() : LocalDateTime.now());

                if (deltas.getViews() == 0 && deltas.getLikes() == 0 && deltas.getFavorites() == 0 && deltas.getComments() == 0) {
                    // nothing to do => mark DONE
                    comp.setStatus("DONE");
                    compensationMapper.updateStatus(comp);
                    continue;
                }

                int updated = noteStatsMapper.incrementByDeltas(deltas);
                if (updated > 0) {
                    comp.setStatus("DONE");
                } else {
                    comp.setRetryCount(comp.getRetryCount() + 1);
                    comp.setStatus(comp.getRetryCount() >= MAX_RETRY ? "FAILED" : "PENDING");
                }
            } catch (Exception e) {
                log.error("compensation retry exception for id={}", comp.getId(), e);
                comp.setRetryCount(comp.getRetryCount() + 1);
                if (comp.getRetryCount() >= MAX_RETRY) comp.setStatus("FAILED");
            }
            compensationMapper.updateStatus(comp);
        }
    }
}
