package top.itning.generic.service.core.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.websocket.*;
import javax.websocket.server.ServerEndpoint;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 日志输出
 *
 * @author itning
 */
@Component
@ServerEndpoint(value = "/log")
public final class LogWebSocket {
    private static final Logger logger = LoggerFactory.getLogger(LogWebSocket.class);
    /**
     * 存放Session
     */
    private static final Map<String, Session> SESSION_MAP = new ConcurrentHashMap<>(16);
    /**
     * OutputStream
     */
    private static final ByteArrayOutputStream SEND_STREAM = new ByteArrayOutputStream();

    /**
     * 获取输出流
     *
     * @return OutputStream
     */
    public synchronized static ByteArrayOutputStream getOutputStream() {
        return SEND_STREAM;
    }

    /**
     * 将日志信息写入WebSocket
     */
    @Scheduled(fixedDelay = 500)
    private synchronized static void writeLog2WebSocket() {
        //ByteArrayOutputStream May Bigger?
        if (SESSION_MAP.isEmpty()) {
            SEND_STREAM.reset();
            return;
        }
        try {
            byte[] toByteArray = SEND_STREAM.toByteArray();
            if (toByteArray.length != 0) {
                ByteArrayInputStream swapStream = new ByteArrayInputStream(toByteArray);
                byte[] bytes = new byte[swapStream.available()];
                int read = swapStream.read(bytes);
                if (read != -1) {
                    //remove un open in session list
                    clearSessionMap();
                    for (Session s : SESSION_MAP.values()) {
                        s.getBasicRemote().sendBinary(ByteBuffer.wrap(bytes));
                    }
                }
            }
        } catch (Exception e) {
            //
        } finally {
            SEND_STREAM.reset();
        }
    }

    /**
     * 清理SessionMap
     */
    private synchronized static void clearSessionMap() {
        SESSION_MAP.values().stream()
                .filter(session -> !session.isOpen())
                //must collect else will throw ConcurrentModificationException
                .collect(Collectors.toList())
                .forEach(session -> SESSION_MAP.remove(session.getId()));
    }

    @OnOpen
    public void onOpen(Session session) {
        SESSION_MAP.put(session.getId(), session);
    }

    @OnClose
    public void onClose(CloseReason closeReason, Session session) {
        logger.debug("On Close Session Id: {} Close Reason: {}", session.getId(), CloseReason.CloseCodes.getCloseCode(closeReason.getCloseCode().getCode()));
    }

    @OnMessage
    public void onMessage(String message, Session session) throws IOException {
        logger.debug("onMessage {}", message);
        //回复用户
        session.getBasicRemote().sendText("收到消息 ");
    }

    @OnError
    public void onError(Session session, Throwable error) {
        SESSION_MAP.remove(session.getId());
        logger.warn("onError ", error);
    }
}
