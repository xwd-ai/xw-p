package com.xwp.engine.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 通知 worker 骨架。
 *
 * <p>Phase 0 仅占位：后续接入 RabbitMQ 监听器，从队列消费消息并 mock 发送。</p>
 */
@Component
public class NotificationWorker {

    private static final Logger log = LoggerFactory.getLogger(NotificationWorker.class);

    public void handle(String triggerId) {
        log.info("[mock] worker 收到消息 triggerId={}", triggerId);
    }
}
