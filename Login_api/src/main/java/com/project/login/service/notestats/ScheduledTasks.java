package com.project.login.service.notestats;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ScheduledTasks {

    private final NoteStatsService noteStatsService;
    private final CompensationService compensationService;

    // 每5分钟批量 flush Redis -> MQ（可根据负载调节）
    @Scheduled(cron = "0 */30 * * * *")
    public void flushRedisToMQ() {
        noteStatsService.flushToMQ();
    }

    // 每分钟处理补偿表
    @Scheduled(cron = "0 */1 * * * *")
    public void processCompensation() {
        compensationService.retryCompensation();
    }
}
