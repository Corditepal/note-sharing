package com.project.login.service.noting;

import com.project.login.model.event.EsNoteEvent;
import jakarta.annotation.Resource;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

@Component
public class NoteEventPublisher {

    @Resource
    private RabbitTemplate rabbitTemplate;

    public void sendEsNoteEvent(EsNoteEvent event) {
        rabbitTemplate.convertAndSend("note.es.queue", event);
    }

    public void sendRedisNoteEvent(EsNoteEvent event) {
        rabbitTemplate.convertAndSend("note.redis.queue", event);
    }

    public void sendMongoNoteEvent(EsNoteEvent event) {
        rabbitTemplate.convertAndSend("note.mongo.queue", event);
    }
}
