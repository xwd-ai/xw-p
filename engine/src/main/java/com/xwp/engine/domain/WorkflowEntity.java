package com.xwp.engine.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.OffsetDateTime;

/**
 * 工作流（通知模板）JPA 实体，对应表 {@code workflows}。
 *
 * <p>字段与 Python 侧 {@code api.app.models.db.WorkflowModel} 及迁移脚本
 * {@code scripts/migrations/002_core_entities.sql} 保持一致：
 * 主键为业务侧生成的 UUID 字符串，时间戳为 {@code TIMESTAMPTZ}，
 * {@code steps} 以 JSONB 承载可变结构。</p>
 */
@Entity
@Table(name = "workflows")
public class WorkflowEntity {

    @Id
    @Column(name = "id", length = 36, nullable = false)
    private String id;

    @Column(name = "name", length = 255, nullable = false, unique = true)
    private String name;

    @Column(name = "description", columnDefinition = "text")
    private String description;

    /** JSONB 承载的有序步骤列表。 */
    @Column(name = "steps", columnDefinition = "jsonb", nullable = false)
    private String steps;

    @Column(name = "version", nullable = false)
    private Integer version;

    @Column(name = "environment", length = 32, nullable = false)
    private String environment;

    @Column(name = "active", nullable = false)
    private Boolean active;

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;

    protected WorkflowEntity() {
        // JPA 需要无参构造器
    }

    public WorkflowEntity(String id, String name, String description, String steps,
                          Integer version, String environment, Boolean active,
                          OffsetDateTime createdAt) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.steps = steps;
        this.version = version;
        this.environment = environment;
        this.active = active;
        this.createdAt = createdAt;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public String getSteps() {
        return steps;
    }

    public Integer getVersion() {
        return version;
    }

    public String getEnvironment() {
        return environment;
    }

    public Boolean getActive() {
        return active;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }
}
