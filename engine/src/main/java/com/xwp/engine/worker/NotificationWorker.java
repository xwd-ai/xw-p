package com.xwp.engine.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xwp.engine.queue.QueuedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

/**
 * 通知 worker：从 Redis 队列消费消息并 mock 发送。
 *
 * <p>Phase 0 阶段：与 Python API 侧 {@code RedisMessageBus}（LPUSH 入队）配对，
 * 使用 {@code BRPOP} 从队列右侧阻塞弹出，反序列化为 {@link QueuedMessage}，
 * 打印 mock 发送日志。真正的 provider 分发与落库在后续阶段接入。</p>
 *
 * <p>消费线程为独立非守护线程，保证 {@link CommandLineRunner#run} 返回后
 * 应用进程不退出，worker 持续监听队列。</p>
 */
@Component
public class NotificationWorker implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(NotificationWorker.class);

    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;

    @Value("${xw.queue-key}")
    private String queueKey;

    @Value("${xw.poll-timeout:2}")
    private int pollTimeout;

    public NotificationWorker(StringRedisTemplate redis, ObjectMapper objectMapper) {
        this.redis = redis;
        this.objectMapper = objectMapper;
    }

    @Override
    public void run(String... args) {
        Thread consumer = new Thread(this::consumeLoop, "xw-engine-consumer");
        consumer.setDaemon(false);
        consumer.start();
        log.info("[engine] worker 已启动，监听队列 key={}，阻塞超时={}s", queueKey, pollTimeout);
    }

    private void consumeLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                // BRPOP 从队列右侧阻塞弹出；超时返回 null，继续下一轮
                String payload = redis.opsForList().rightPop(queueKey, Duration.ofSeconds(pollTimeout));
                if (payload == null) {
                    continue;
                }
                QueuedMessage message = objectMapper.readValue(payload, QueuedMessage.class);
                dispatch(message);
            } catch (Exception e) {
                log.warn("[engine] 消费消息失败，跳过并继续：{}", e.getMessage());
            }
        }
    }

    private void dispatch(QueuedMessage message) {
        List<String> channels = message.channels() == null || message.channels().isEmpty()
                ? List.of("in_app")
                : message.channels();
        log.info(
                "[mock] 发送通知 triggerId={} workflow={} -> subscriberId={} channels={} payload={}",
                message.triggerId(), message.name(), message.subscriberId(), channels, message.payload());
    }
}
