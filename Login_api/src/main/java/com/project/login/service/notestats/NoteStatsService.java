package com.project.login.service.notestats;

import com.project.login.mapper.NoteStatsCompensationMapper;
import com.project.login.mapper.NoteStatsMapper;
import com.project.login.model.dataobject.NoteStatsDO;
import com.project.login.model.vo.NoteStatsVO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class NoteStatsService {

    private final RedisTemplate<String, Object> redisTemplate;
    private final NoteStatsMapper noteStatsMapper;
    private final NoteStatsCompensationMapper compensationMapper;
    private final RabbitTemplate rabbitTemplate;

    private static final String REDIS_KEY_PREFIX = "note_stats:";
    private static final String MQ_QUEUE = "note.redis.queue";

    /**
     * 高频写入（写 Redis 总量）。若 Redis 无数据则从 DB 读并回写（包含 version）。
     */
    public NoteStatsVO changeField(Long noteId, String field, long delta) {
        if (noteId == null || noteId < 1) {
            log.warn("Invalid noteId={}, skip changeField", noteId);
            return emptyStats(noteId);
        }

        String key = REDIS_KEY_PREFIX + noteId;
        HashOperations<String, Object, Object> ops = redisTemplate.opsForHash();

        // 如果 Redis 无数据，则初始化：从 DB 读取并回写（或在 DB 中插入空行）
        if (!redisTemplate.hasKey(key) || ops.size(key) == 0) {
            NoteStatsDO db = noteStatsMapper.getById(noteId);
            if (db == null) {
                // 如果 DB 没有，插入初始行（version = 0）
                NoteStatsDO init = new NoteStatsDO();
                init.setNoteId(noteId);
                init.setViews(0L);
                init.setLikes(0L);
                init.setFavorites(0L);
                init.setComments(0L);
                init.setLastActivityAt(LocalDateTime.now());
                init.setVersion(0L);
                noteStatsMapper.insert(init);
                db = noteStatsMapper.getById(noteId);
            }
            // 回写 Redis（将 DB 总量写进 Redis，并写入 base version）
            ops.put(key, "views", db.getViews());
            ops.put(key, "likes", db.getLikes());
            ops.put(key, "favorites", db.getFavorites());
            ops.put(key, "comments", db.getComments());
            ops.put(key, "last_activity_at", db.getLastActivityAt().toString());
            ops.put(key, "version", String.valueOf(db.getVersion()));
            redisTemplate.expire(key, 7, TimeUnit.DAYS);
        }

        // 原子增量
        ops.increment(key, field, delta);
        ops.put(key, "last_activity_at", LocalDateTime.now().toString());

        // 返回当前值（从 Redis 读）
        Map<Object, Object> map = ops.entries(key);
        return toVO(noteId, map);
    }

    /**
     * 获取 Redis 中总量；若 Redis 没有则从 DB 读并回写 Redis
     */
    public NoteStatsVO getStats(Long noteId) {
        if (noteId == null || noteId < 1) {
            log.warn("Invalid noteId={}, return emptyStats", noteId);
            return emptyStats(noteId);
        }

        String key = REDIS_KEY_PREFIX + noteId;
        HashOperations<String, Object, Object> ops = redisTemplate.opsForHash();
        Map<Object, Object> map = ops.entries(key);

        if (map.isEmpty()) {
            NoteStatsDO db = noteStatsMapper.getById(noteId);
            if (db == null) return emptyStats(noteId);
            // 回写 Redis
            ops.put(key, "views", db.getViews());
            ops.put(key, "likes", db.getLikes());
            ops.put(key, "favorites", db.getFavorites());
            ops.put(key, "comments", db.getComments());
            ops.put(key, "last_activity_at", db.getLastActivityAt().toString());
            ops.put(key, "version", String.valueOf(db.getVersion()));
            redisTemplate.expire(key, 7, TimeUnit.DAYS);
            map = ops.entries(key);
        }

        return toVO(noteId, map);
    }

    private NoteStatsVO toVO(Long noteId, Map<Object, Object> map) {
        NoteStatsVO vo = new NoteStatsVO();
        vo.setNoteId(noteId);
        vo.setViews(parseLong(map.get("views")));
        vo.setLikes(parseLong(map.get("likes")));
        vo.setFavorites(parseLong(map.get("favorites")));
        vo.setComments(parseLong(map.get("comments")));
        return vo;
    }

    private long parseLong(Object o) {
        if (o == null) return 0L;
        if (o instanceof Number) return ((Number) o).longValue();
        try { return Long.parseLong(o.toString()); } catch (Exception ex) { return 0L; }
    }

    private NoteStatsVO emptyStats(Long noteId) {
        NoteStatsVO vo = new NoteStatsVO();
        vo.setNoteId(noteId);
        vo.setViews(0L);
        vo.setLikes(0L);
        vo.setFavorites(0L);
        vo.setComments(0L);
        return vo;
    }

    /**
     * 批量 flush Redis -> MQ（推送每个 key 的总量 + base version）
     */
    public void flushToMQ() {
        Set<String> keys = redisTemplate.keys(REDIS_KEY_PREFIX + "*");
        if (keys.isEmpty()) return;

        keys.forEach(key -> {
            try {
                Long noteId = Long.parseLong(key.substring(REDIS_KEY_PREFIX.length()));
                if (noteId < 1) {
                    log.warn("Invalid noteId={}, skip flushToMQ", noteId);
                    return;
                }

                Map<Object, Object> map = redisTemplate.opsForHash().entries(key);
                if (map.isEmpty()) return;

                Map<String, Object> msg = new HashMap<>();
                msg.put("note_id", noteId);
                msg.put("views", parseLong(map.get("views")));
                msg.put("likes", parseLong(map.get("likes")));
                msg.put("favorites", parseLong(map.get("favorites")));
                msg.put("comments", parseLong(map.get("comments")));
                msg.put("last_activity_at", map.getOrDefault("last_activity_at", LocalDateTime.now().toString()).toString());
                msg.put("version", map.getOrDefault("version", "0").toString());

                rabbitTemplate.convertAndSend(MQ_QUEUE, msg);
                log.debug("Flushed note stats to MQ: noteId={} msg={}", noteId, msg);
            } catch (Exception e) {
                log.error("flushToMQ error for key {}", key, e);
            }
        });
    }

    /**
     * 启动时异步预热：从 DB 读 top-n 最近更新的数据写回 Redis
     */
    @Async
    public void preloadRecent(int n) {
        List<NoteStatsDO> list = noteStatsMapper.getRecentUpdated(n);
        Random rnd = new Random();
        for (NoteStatsDO item : list) {
            if (item.getNoteId() < 1) continue; // 安全兜底

            try { Thread.sleep(rnd.nextInt(100)); } catch (InterruptedException ignored) {}
            String key = REDIS_KEY_PREFIX + item.getNoteId();
            HashOperations<String, Object, Object> ops = redisTemplate.opsForHash();
            ops.put(key, "views", item.getViews());
            ops.put(key, "likes", item.getLikes());
            ops.put(key, "favorites", item.getFavorites());
            ops.put(key, "comments", item.getComments());
            ops.put(key, "last_activity_at", item.getLastActivityAt().toString());
            ops.put(key, "version", String.valueOf(item.getVersion()));
            redisTemplate.expire(key, 7, TimeUnit.DAYS);
        }
    }
}
