package com.xwp.engine.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.OffsetDateTime;

/**
 * 订阅者 JPA 实体，对应表 {@code subscribers}。
 *
 * <p>字段与 Python 侧 {@code api.app.models.db.SubscriberModel} 对齐，
 * {@code channels} 以 JSONB 承载启用的渠道列表。</p>
 */
@Entity
@Table(name = "subscribers")
public class SubscriberEntity {

    @Id
    @Column(name = "id", length = 36, nullable = false)
    private String id;

    @Column(name = "email", length = 255)
    private String email;

    @Column(name = "phone", length = 64)
    private String phone;

    @Column(name = "device_token", length = 512)
    private String deviceToken;

    /** JSONB 承载的启用渠道列表。 */
    @Column(name = "channels", columnDefinition = "jsonb", nullable = false)
    private String channels;

    @Column(name = "environment", length = 32, nullable = false)
    private String environment;

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;

    protected SubscriberEntity() {
        // JPA 需要无参构造器
    }

    public SubscriberEntity(String id, String email, String phone, String deviceToken,
                            String channels, String environment, OffsetDateTime createdAt) {
        this.id = id;
        this.email = email;
        this.phone = phone;
        this.deviceToken = deviceToken;
        this.channels = channels;
        this.environment = environment;
        this.createdAt = createdAt;
    }

    public String getId() {
        return id;
    }

    public String getEmail() {
        return email;
    }

    public String getPhone() {
        return phone;
    }

    public String getDeviceToken() {
        return deviceToken;
    }

    public String getChannels() {
        return channels;
    }

    public String getEnvironment() {
        return environment;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }
}
