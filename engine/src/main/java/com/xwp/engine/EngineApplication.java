package com.xwp.engine;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;

/**
 * xw-notify 执行引擎入口。
 *
 * <p>负责消费消息队列、执行工作流求值、digest 聚合与 provider 分发。
 * Phase 0 阶段：启动 Web 与 Actuator，并由 {@code NotificationWorker} 从 Redis
 * 队列消费消息（mock 发送）。</p>
 *
 * <p>默认排除数据源/JPA/Flyway 自动装配，使引擎在仅部署 Redis 时即可启动；
 * 领域模型的持久化层（JPA + Flyway）由 {@code xw.datasource.enabled=true} 显式
 * 开启（见 {@code JpaConfig}）。</p>
 */
@SpringBootApplication(exclude = {
        DataSourceAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class,
        FlywayAutoConfiguration.class,
})
public class EngineApplication {

    public static void main(String[] args) {
        SpringApplication.run(EngineApplication.class, args);
    }
}
