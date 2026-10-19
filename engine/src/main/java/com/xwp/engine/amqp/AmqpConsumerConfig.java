package com.xwp.engine.amqp;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.config.RetryInterceptorBuilder;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.retry.RejectAndDontRequeueRecoverer;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.interceptor.RetryOperationsInterceptor;

/**
 * RabbitMQ 消费框架配置：声明主队列、重试队列与死信队列（DLX）。
 *
 * <p>设计约定：</p>
 * <ul>
 *   <li>主队列绑定死信交换机，消费失败且重试耗尽的消息进入死信队列；</li>
 *   <li>重试队列设置 TTL，到期重新投递回主队列，实现延迟重试；</li>
 *   <li>JSON 消息转换器负责 {@code QueuedMessage} 的序列化/反序列化。</li>
 * </ul>
 *
 * <p>通过 {@code xw.rabbitmq.enabled=false}（默认）关闭，此时不声明任何 Bean，
 * 避免在无 RabbitMQ 的环境启动报错。真实接入时通过环境变量开启并配置连接。</p>
 */
@Configuration
@ConditionalOnProperty(name = "xw.rabbitmq.enabled", havingValue = "true")
public class AmqpConsumerConfig {

    public static final String EXCHANGE = "xw.exchange.events";
    public static final String QUEUE = "xw.queue.events";
    public static final String RETRY_QUEUE = "xw.queue.events.retry";
    public static final String DLX = "xw.exchange.dlx";
    public static final String DLQ = "xw.queue.events.dlq";
    public static final String RETRY_ROUTING_KEY = "xw.retry";
    public static final String DEAD_ROUTING_KEY = "xw.dead";

    @Value("${xw.rabbitmq.retry-ttl:5000}")
    private long retryTtl;

    @Value("${xw.rabbitmq.max-retries:3}")
    private int maxRetries;

    @Bean
    public DirectExchange eventsExchange() {
        return new DirectExchange(EXCHANGE, true, false);
    }

    @Bean
    public DirectExchange dlxExchange() {
        return new DirectExchange(DLX, true, false);
    }

    /** 主队列：绑定死信交换机，消费失败且重试耗尽的消息转发到 DLX。 */
    @Bean
    public Queue eventsQueue() {
        return QueueBuilder.durable(QUEUE)
                .withArgument("x-dead-letter-exchange", DLX)
                .withArgument("x-dead-letter-routing-key", DEAD_ROUTING_KEY)
                .build();
    }

    /** 重试队列：TTL 到期后消息重新投递回主队列，实现延迟重试。 */
    @Bean
    public Queue retryQueue() {
        return QueueBuilder.durable(RETRY_QUEUE)
                .withArgument("x-dead-letter-exchange", EXCHANGE)
                .withArgument("x-dead-letter-routing-key", RETRY_ROUTING_KEY)
                .withArgument("x-message-ttl", retryTtl)
                .build();
    }

    /** 死信队列：重试耗尽后的终态堆积，供人工排查。 */
    @Bean
    public Queue deadLetterQueue() {
        return QueueBuilder.durable(DLQ).build();
    }

    @Bean
    public Binding eventsBinding() {
        return BindingBuilder.bind(eventsQueue()).to(eventsExchange()).with(RETRY_ROUTING_KEY);
    }

    @Bean
    public Binding retryBinding() {
        return BindingBuilder.bind(retryQueue()).to(dlxExchange()).with(DEAD_ROUTING_KEY);
    }

    @Bean
    public Binding deadLetterBinding() {
        return BindingBuilder.bind(deadLetterQueue()).to(dlxExchange()).with(DEAD_ROUTING_KEY);
    }

    @Bean
    public MessageConverter jacksonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(jacksonMessageConverter());
        return template;
    }

    /**
     * 监听容器工厂：为消费处理附加重试拦截器。
     *
     * <p>消费异常时最多重试 {@code xw.rabbitmq.max-retries} 次（默认 3 次，
     * 指数退避）。重试耗尽后 {@link RejectAndDontRequeueRecoverer} 拒绝消息且
     * 不重新入队，消息随主队列的 DLX 配置进入死信队列。</p>
     */
    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(jacksonMessageConverter());

        RetryOperationsInterceptor interceptor = RetryInterceptorBuilder.stateless()
                .maxAttempts(maxRetries)
                .backOffOptions(1000, 2.0, 10000)
                .recoverer(new RejectAndDontRequeueRecoverer())
                .build();
        factory.setAdviceChain(interceptor);
        return factory;
    }
}
