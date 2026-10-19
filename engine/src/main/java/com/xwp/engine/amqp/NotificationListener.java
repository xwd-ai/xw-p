package com.xwp.engine.amqp;

import com.xwp.engine.queue.QueuedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * RabbitMQ 消息监听器：消费 {@link QueuedMessage} 并 mock 投递。
 *
 * <p>消费失败抛出异常时由 RabbitMQ 的 {@code retry interceptor} 进行有限次重试，
 * 重试耗尽后经死信交换机进入死信队列（见 {@link AmqpConsumerConfig}）。
 * 当前为 Phase 1 占位实现：仅记录 mock 投递日志，真正的 provider 分发与
 * 消息状态机落库在后续 Phase 接入。</p>
 *
 * <p>通过 {@code xw.rabbitmq.enabled=false}（默认）关闭监听，避免在未部署
 * RabbitMQ 的环境中启动报错。</p>
 */
@Component
@ConditionalOnProperty(name = "xw.rabbitmq.enabled", havingValue = "true")
public class NotificationListener {

    private static final Logger log = LoggerFactory.getLogger(NotificationListener.class);

    /**
     * 消费主队列消息。默认手动确认由容器管理；处理异常触发重试机制。
     */
    @RabbitListener(queues = AmqpConsumerConfig.QUEUE)
    public void onMessage(QueuedMessage message, Message raw) {
        List<String> channels = message.channels() == null || message.channels().isEmpty()
                ? List.of("in_app")
                : message.channels();
        log.info(
                "[amqp] 消费通知 triggerId={} workflow={} -> subscriberId={} channels={} payload={}",
                message.triggerId(), message.name(), message.subscriberId(), channels, message.payload());
        // TODO(后续 Phase)：接入 provider 分发与消息状态机，失败抛异常触发重试。
    }
}
