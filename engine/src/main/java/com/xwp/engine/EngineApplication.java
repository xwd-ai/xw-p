package com.xwp.engine;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * xw-notify 执行引擎入口。
 *
 * <p>负责消费消息队列、执行工作流求值、digest 聚合与 provider 分发。
 * 当前为骨架：仅启动 Web 与 Actuator，消费者在后续阶段接入。</p>
 */
@SpringBootApplication
public class EngineApplication {

    public static void main(String[] args) {
        SpringApplication.run(EngineApplication.class, args);
    }
}
