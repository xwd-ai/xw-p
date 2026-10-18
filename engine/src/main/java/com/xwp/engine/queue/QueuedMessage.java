package com.xwp.engine.queue;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * 队列中的通知消息，与 Python API 侧 {@code QueuedMessage} 的 JSON 结构一一对应。
 *
 * <p>Python 侧使用 Pydantic {@code model_dump()} 序列化为 snake_case 字段，
 * 此处用 {@link JsonProperty} 显式映射，避免依赖字段命名策略。</p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record QueuedMessage(
        @JsonProperty("trigger_id") String triggerId,
        @JsonProperty("transaction_id") String transactionId,
        @JsonProperty("name") String name,
        @JsonProperty("subscriber_id") String subscriberId,
        @JsonProperty("email") String email,
        @JsonProperty("phone") String phone,
        @JsonProperty("channels") List<String> channels,
        @JsonProperty("payload") Map<String, Object> payload,
        @JsonProperty("overrides") Map<String, Object> overrides
) {
}
