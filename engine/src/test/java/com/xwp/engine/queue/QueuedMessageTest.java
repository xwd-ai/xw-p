package com.xwp.engine.queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 验证 {@link QueuedMessage} 与 Python 侧 JSON 结构的反序列化映射。
 *
 * <p>使用与 Python {@code QueuedMessage.model_dump()} 相同的 snake_case 字段，
 * 确保跨语言消息契约一致。</p>
 */
class QueuedMessageTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void deserializesSnakeCasePayload() throws Exception {
        String json = """
                {
                  "trigger_id": "t-001",
                  "transaction_id": "x-001",
                  "name": "welcome",
                  "subscriber_id": "user_1",
                  "email": "hi@example.com",
                  "phone": null,
                  "channels": ["in_app", "email"],
                  "payload": {"user": "Alice"},
                  "overrides": {}
                }
                """;

        QueuedMessage message = mapper.readValue(json, QueuedMessage.class);

        assertThat(message.triggerId()).isEqualTo("t-001");
        assertThat(message.transactionId()).isEqualTo("x-001");
        assertThat(message.name()).isEqualTo("welcome");
        assertThat(message.subscriberId()).isEqualTo("user_1");
        assertThat(message.email()).isEqualTo("hi@example.com");
        assertThat(message.channels()).isEqualTo(List.of("in_app", "email"));
        assertThat(message.payload()).isEqualTo(Map.of("user", "Alice"));
    }

    @Test
    void toleratesUnknownFields() throws Exception {
        String json = """
                {
                  "trigger_id": "t-002",
                  "transaction_id": "x-002",
                  "name": "welcome",
                  "subscriber_id": "user_2",
                  "future_field": {"anything": true}
                }
                """;

        QueuedMessage message = mapper.readValue(json, QueuedMessage.class);

        assertThat(message.triggerId()).isEqualTo("t-002");
        assertThat(message.channels()).isNull();
    }
}
