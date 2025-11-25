package com.project.login.convert;

import com.project.login.model.dto.search.NoteSearchDTO;
import com.project.login.model.request.search.NoteSearchRequest;
import com.project.login.model.vo.NoteSearchVO;
import org.mapstruct.Mapper;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Mapper(componentModel = "spring")
public interface SearchConvert {

    // 请求对象转换
    NoteSearchDTO toSearchDTO(NoteSearchRequest request);

    // ES 返回 Object 转 VO（ES 返回的是 Map 风格）
    default NoteSearchVO toSearchVO(Object source) {
        if (source == null) return null;

        NoteSearchVO vo = new NoteSearchVO();

        // 假设 ES 返回的是 Map<String, Object>
        Map<String, Object> map = (Map<String, Object>) source;

        vo.setNoteId(map.get("id") != null ? Long.valueOf(map.get("id").toString()) : null);
        vo.setTitle((String) map.get("title"));
        vo.setContentSummary((String) map.get("content_summary")); // 对应 VO 字段
        vo.setAuthorName((String) map.get("authorName"));

        // 统计字段，null 默认 0
        vo.setViewCount(map.get("viewCount") != null ? Integer.valueOf(map.get("viewCount").toString()) : 0);
        vo.setLikeCount(map.get("likeCount") != null ? Integer.valueOf(map.get("likeCount").toString()) : 0);
        vo.setFavoriteCount(map.get("favoriteCount") != null ? Integer.valueOf(map.get("favoriteCount").toString()) : 0);
        vo.setCommentCount(map.get("commentCount") != null ? Integer.valueOf(map.get("commentCount").toString()) : 0);

        // 更新时间
        if (map.get("updatedAt") != null) {
            vo.setUpdatedAt(LocalDateTime.parse(map.get("updatedAt").toString()));
        }

        return vo;
    }

}
