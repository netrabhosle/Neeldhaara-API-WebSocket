package com.aquora.mysql_backend.websocket;

import com.aquora.mysql_backend.dto.BorewellReadingResponse;
import com.aquora.mysql_backend.entity.BorewellReadings;
import com.aquora.mysql_backend.repository.BorewellReadingsRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.*;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class BorewellWebSocketHandler extends TextWebSocketHandler {

    private final Set<WebSocketSession> sessions =
            ConcurrentHashMap.newKeySet();

    private final ObjectMapper mapper;
    private final BorewellReadingsRepository repo;

    public BorewellWebSocketHandler(
            ObjectMapper mapper,
            BorewellReadingsRepository repo) {

        this.mapper = mapper;
        this.repo = repo;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession s) {

        sessions.add(s);

        // Send latest database reading immediately
        repo.findTopByOrderByIdDesc().ifPresent(reading -> {
            try {
                BorewellReadingResponse data =
                        BorewellReadingResponse.from(reading);

                s.sendMessage(
                        new TextMessage(
                                mapper.writeValueAsString(data)
                        )
                );

            } catch (Exception e) {
                sessions.remove(s);
            }
        });
    }

    @Override
    public void afterConnectionClosed(
            WebSocketSession s,
            CloseStatus status) {

        sessions.remove(s);
    }

    public void broadcast(BorewellReadingResponse data) {

        try {
            TextMessage msg =
                    new TextMessage(
                            mapper.writeValueAsString(data)
                    );

            for (WebSocketSession s : sessions) {

                if (s.isOpen()) {

                    try {
                        s.sendMessage(msg);
                    } catch (Exception e) {
                        sessions.remove(s);
                    }
                }
            }

        } catch (Exception ignored) {
        }
    }
}

//Websocket