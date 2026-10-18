package com.xwp.engine;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * xw-notify 执行引擎入口。
 *
 * <p>负责消费消息队列、执行工作流求值、digest 聚合与 provider 分发。
 * Phase 0 阶段：启动 Web 与 Actuator，并由 {@code NotificationWorker} 从 Redis
 * 队列消费消息（mock 发送）。</p>
 */
@SpringBootApplication
public class EngineApplication {

    public static void main(String[] args) {
        SpringApplication.run(EngineApplication.class, args);
    }
}
