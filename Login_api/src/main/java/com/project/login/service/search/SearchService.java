package com.project.login.service.search;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.FunctionBoostMode;
import co.elastic.clients.elasticsearch._types.query_dsl.FunctionScoreMode;
import com.project.login.convert.SearchConvert;
import com.project.login.model.dto.search.NoteSearchDTO;
import com.project.login.model.vo.NoteSearchVO;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class SearchService {

    private final ElasticsearchClient esClient;

    @Qualifier("searchConvert")
    private final SearchConvert convert;

    public List<NoteSearchVO> searchNotes(NoteSearchDTO dto) {
        String keyword = dto.getKeyword();

        try {
            var response = esClient.search(s -> s
                            .index("notes")
                            .size(30)
                            .query(q -> q
                                    .functionScore(fs -> fs
                                            .query(base -> base
                                                    .multiMatch(m -> m
                                                            .query(keyword)
                                                            .fields("title^2", "content_summary")
                                                    )
                                            )
                                            .boostMode(FunctionBoostMode.Sum)
                                            .scoreMode(FunctionScoreMode.Sum)
                                            .functions(fns -> fns
                                                    .scriptScore(sc -> sc
                                                            .script(scr -> scr
                                                                    .source("""
                                                                        // ========== 综合评分公式 ==========
                                                                        
                                                                        double safeLong(def field) {
                                                                            if (field == null) return 0;
                                                                            return field.value;
                                                                        }

                                                                        double baseScore = _score * 0.5;

                                                                        double views = safeLong(doc['views']);
                                                                        double likes = safeLong(doc['likes']);
                                                                        double viewsScore = Math.log(views + 1) * 0.2;
                                                                        double likesScore = Math.log(likes + 1) * 0.2;

                                                                        long updated = 0L;
                                                                        if (doc.containsKey("updatedAt") && doc['updatedAt'].size() > 0) {
                                                                            updated = doc['updatedAt'].value.toInstant().toEpochMilli();
                                                                        }

                                                                        long now = System.currentTimeMillis();
                                                                        double days = (now - updated) / 86400000.0;
                                                                        double recency = 1.0 / (days + 1.0);
                                                                        double recencyScore = recency * 0.1;

                                                                        return baseScore + viewsScore + likesScore + recencyScore;
                                                                    """)
                                                                    .lang("painless")
                                                            )
                                                    )
                                            )
                                    )
                            ),
                    Object.class
            );

            return response.hits()
                    .hits()
                    .stream()
                    .map(hit -> convert.toSearchVO(hit.source()))
                    .collect(Collectors.toList());

        } catch (IOException e) {
            throw new RuntimeException("搜索失败", e);
        }
    }
}
