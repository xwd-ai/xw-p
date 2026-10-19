package com.xwp.engine.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.OffsetDateTime;

/**
 * 消息 JPA 实体，对应表 {@code messages}（核心领域消息模型）。
 *
 * <p>字段与 Python 侧 {@code api.app.models.db.MessageModel} 对齐。
 * {@code status} 存 {@link MessageStatus} 的字符串值，
 * {@code payload} 以 JSONB 承载渲染后的消息内容。</p>
 */
@Entity
@Table(name = "messages")
public class MessageEntity {

    @Id
    @Column(name = "id", length = 36, nullable = false)
    private String id;

    @Column(name = "workflow_id", length = 36, nullable = false)
    private String workflowId;

    @Column(name = "subscriber_id", length = 36, nullable = false)
    private String subscriberId;

    @Column(name = "channel", length = 32, nullable = false)
    private String channel;

    /** JSONB 承载的渲染后消息内容。 */
    @Column(name = "payload", columnDefinition = "jsonb", nullable = false)
    private String payload;

    /** 存 {@link MessageStatus} 的字符串值。 */
    @Column(name = "status", length = 32, nullable = false)
    private String status;

    @Column(name = "error", columnDefinition = "text")
    private String error;

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    protected MessageEntity() {
        // JPA 需要无参构造器
    }

    public MessageEntity(String id, String workflowId, String subscriberId, String channel,
                         String payload, String status, String error,
                         OffsetDateTime createdAt, OffsetDateTime updatedAt) {
        this.id = id;
        this.workflowId = workflowId;
        this.subscriberId = subscriberId;
        this.channel = channel;
        this.payload = payload;
        this.status = status;
        this.error = error;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public String getId() {
        return id;
    }

    public String getWorkflowId() {
        return workflowId;
    }

    public String getSubscriberId() {
        return subscriberId;
    }

    public String getChannel() {
        return channel;
    }

    public String getPayload() {
        return payload;
    }

    public String getStatus() {
        return status;
    }

    public String getError() {
        return error;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }

    public OffsetDateTime getUpdatedAt() {
        return updatedAt;
    }
}
