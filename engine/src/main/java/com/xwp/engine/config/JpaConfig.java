package com.xwp.engine.config;

import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;

import jakarta.persistence.EntityManagerFactory;
import javax.sql.DataSource;

import java.util.Properties;

/**
 * 引擎侧持久化配置：数据源 + Flyway 迁移 + JPA。
 *
 * <p>默认关闭（与 {@code EngineApplication} 排除数据源/JPA/Flyway 自动装配相呼应），
 * 使引擎在仅部署 Redis 时也能启动。需要落库时通过
 * {@code xw.datasource.enabled=true} 开启，并注入 Postgres 连接信息。</p>
 */
@Configuration
@ConditionalOnProperty(name = "xw.datasource.enabled", havingValue = "true")
public class JpaConfig {

    @Value("${xw.datasource.url}")
    private String url;

    @Value("${xw.datasource.username}")
    private String username;

    @Value("${xw.datasource.password:}")
    private String password;

    @Bean
    public DataSource dataSource() {
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(url);
        ds.setUsername(username);
        ds.setPassword(password);
        ds.setDriverClassName("org.postgresql.Driver");
        return ds;
    }

    @Bean
    public LocalContainerEntityManagerFactoryBean entityManagerFactory(DataSource dataSource) {
        LocalContainerEntityManagerFactoryBean emf = new LocalContainerEntityManagerFactoryBean();
        emf.setDataSource(dataSource);
        emf.setPackagesToScan("com.xwp.engine.domain");
        emf.setJpaVendorAdapter(new HibernateJpaVendorAdapter());
        Properties props = new Properties();
        props.setProperty("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
        emf.setJpaProperties(props);
        return emf;
    }

    @Bean
    public JpaTransactionManager transactionManager(EntityManagerFactory emf) {
        return new JpaTransactionManager(emf);
    }

    /**
     * 显式执行 Flyway 迁移（因默认 Flyway 自动装配已被排除）。
     */
    @Bean
    public Flyway flyway(DataSource dataSource) {
        Flyway flyway = Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .load();
        flyway.migrate();
        return flyway;
    }
}
