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

    private static final Set<String> ALLOWED_FIELDS =
            Set.of("views", "likes", "favorites", "comments");

    /**
     * 高频写入（写 Redis 总量）。若 Redis 无数据则从 DB 读并回写（包含 version）。
     */
    public NoteStatsVO changeField(Long noteId, String field, long delta) {
        if (noteId == null || noteId < 1) {
            log.warn("Invalid noteId={}, skip changeField", noteId);
            return emptyStats(noteId);
        }

        if (!ALLOWED_FIELDS.contains(field)) {
            log.warn("Invalid field={} for noteId={}, skip", field, noteId);
            return getStats(noteId);
        }

        String key = REDIS_KEY_PREFIX + noteId;
        HashOperations<String, Object, Object> ops = redisTemplate.opsForHash();

        // === Redis 初始化 ===
        initRedisIfNeeded(noteId, ops, key);

        // === 原子增量 ===
        ops.increment(key, field, delta);
        ops.put(key, "last_activity_at", LocalDateTime.now().toString());

        // 返回当前值
        Map<Object, Object> map = ops.entries(key);
        return toVO(noteId, map);
    }

    /**
     * 如果 Redis 中没有数据，则从 DB 初始化回写 Redis
     */
    private void initRedisIfNeeded(Long noteId, HashOperations<String, Object, Object> ops, String key) {
        if (redisTemplate.hasKey(key) && ops.size(key) > 0) {
            return;
        }

        NoteStatsDO db = noteStatsMapper.getById(noteId);
        if (db == null) {
            // DB 初始化（version=0）
            db = NoteStatsDO.builder()
                    .noteId(noteId)
                    .authorName("")
                    .views(0L)
                    .likes(0L)
                    .favorites(0L)
                    .comments(0L)
                    .lastActivityAt(LocalDateTime.now())
                    .version(0L)
                    .build();
            noteStatsMapper.insert(db);
        }

        writeStatsToRedis(key, ops, db);
    }

    /**
     * 写入 Redis（安全版，Hash 的 value 全部转为 String）
     */
    private void writeStatsToRedis(String key, HashOperations<String, Object, Object> ops, NoteStatsDO db) {
        // 将所有字段值统一转成 String，保证 StringRedisSerializer 不会报错
        ops.put(key, "authorName", db.getAuthorName());
        ops.put(key, "views", String.valueOf(db.getViews()));
        ops.put(key, "likes", String.valueOf(db.getLikes()));
        ops.put(key, "favorites", String.valueOf(db.getFavorites()));
        ops.put(key, "comments", String.valueOf(db.getComments()));
        ops.put(key, "last_activity_at", db.getLastActivityAt().toString());
        ops.put(key, "version", String.valueOf(db.getVersion()));

        // 设置过期时间
        redisTemplate.expire(key, 7, TimeUnit.DAYS);
    }


    /**
     * 获取 Redis 状态
     */
    public NoteStatsVO getStats(Long noteId) {
        if (noteId == null || noteId < 1) {
            return emptyStats(noteId);
        }

        String key = REDIS_KEY_PREFIX + noteId;
        HashOperations<String, Object, Object> ops = redisTemplate.opsForHash();
        Map<Object, Object> map = ops.entries(key);

        if (map.isEmpty()) {
            NoteStatsDO db = noteStatsMapper.getById(noteId);
            if (db == null) return emptyStats(noteId);

            writeStatsToRedis(key, ops, db);
            map = ops.entries(key);
        }

        return toVO(noteId, map);
    }

    private NoteStatsVO toVO(Long noteId, Map<Object, Object> map) {
        NoteStatsVO vo = new NoteStatsVO();
        vo.setNoteId(noteId);
        vo.setAuthorName(Objects.toString(map.get("authorName"), ""));
        vo.setViews(parseLong(map.get("views")));
        vo.setLikes(parseLong(map.get("likes")));
        vo.setFavorites(parseLong(map.get("favorites")));
        vo.setComments(parseLong(map.get("comments")));
        return vo;
    }

    private long parseLong(Object o) {
        if (o instanceof Number) return ((Number) o).longValue();
        try { return Long.parseLong(o.toString()); } catch (Exception ex) { return 0L; }
    }

    private NoteStatsVO emptyStats(Long noteId) {
        NoteStatsVO vo = new NoteStatsVO();
        vo.setNoteId(noteId);
        vo.setAuthorName("");
        vo.setViews(0L);
        vo.setLikes(0L);
        vo.setFavorites(0L);
        vo.setComments(0L);
        return vo;
    }

    /**
     * Flush Redis → MQ
     */
    public void flushToMQ() {
        Set<String> keys = redisTemplate.keys(REDIS_KEY_PREFIX + "*");
        if (keys == null || keys.isEmpty()) return;

        for (String key : keys) {
            try {
                Long noteId = Long.parseLong(key.substring(REDIS_KEY_PREFIX.length()));
                if (noteId < 1) {
                    continue;
                }

                Map<Object, Object> map = redisTemplate.opsForHash().entries(key);
                if (map.isEmpty()) continue;

                Map<String, Object> msg = new HashMap<>();
                msg.put("note_id", noteId);
                msg.put("authorName", map.getOrDefault("authorName", ""));
                msg.put("views", parseLong(map.get("views")));
                msg.put("likes", parseLong(map.get("likes")));
                msg.put("favorites", parseLong(map.get("favorites")));
                msg.put("comments", parseLong(map.get("comments")));
                msg.put("last_activity_at", map.getOrDefault("last_activity_at",
                        LocalDateTime.now().toString()));
                msg.put("version", map.getOrDefault("version", 0L));

                rabbitTemplate.convertAndSend(MQ_QUEUE, msg);

            } catch (Exception ex) {
                log.error("flushToMQ error for key={}", key, ex);
            }
        }
    }

    /**
     * 异步预热
     */
    @Async
    public void preloadRecent(int n) {
        List<NoteStatsDO> list = noteStatsMapper.getRecentUpdated(n);
        HashOperations<String, Object, Object> ops = redisTemplate.opsForHash();

        for (NoteStatsDO item : list) {
            if (item.getNoteId() < 1) continue;

            String key = REDIS_KEY_PREFIX + item.getNoteId();
            writeStatsToRedis(key, ops, item);

            try { Thread.sleep(30); } catch (Exception ignored) {}
        }
    }
}
