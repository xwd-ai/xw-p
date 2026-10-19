package com.xwp.engine.domain;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 验证消息状态机 {@link MessageStatus} 的合法转移路径与终态行为。
 */
class MessageStatusTest {

    @Test
    void followsHappyPath() {
        assertThat(MessageStatus.CREATED.canTransitionTo(MessageStatus.QUEUED)).isTrue();
        assertThat(MessageStatus.QUEUED.canTransitionTo(MessageStatus.PROCESSING)).isTrue();
        assertThat(MessageStatus.PROCESSING.canTransitionTo(MessageStatus.SENT)).isTrue();
    }

    @Test
    void allowsFailureShortCircuit() {
        // 入队或处理中任一环节都可直接失败
        assertThat(MessageStatus.CREATED.canTransitionTo(MessageStatus.FAILED)).isTrue();
        assertThat(MessageStatus.QUEUED.canTransitionTo(MessageStatus.FAILED)).isTrue();
        assertThat(MessageStatus.PROCESSING.canTransitionTo(MessageStatus.FAILED)).isTrue();
    }

    @Test
    void allowsRetryFromProcessingBackToQueued() {
        // 投递失败重试：处理中回滚到已入队
        assertThat(MessageStatus.PROCESSING.canTransitionTo(MessageStatus.QUEUED)).isTrue();
    }

    @Test
    void rejectsIllegalTransitions() {
        assertThat(MessageStatus.CREATED.canTransitionTo(MessageStatus.SENT)).isFalse();
        assertThat(MessageStatus.CREATED.canTransitionTo(MessageStatus.PROCESSING)).isFalse();
        assertThat(MessageStatus.QUEUED.canTransitionTo(MessageStatus.CREATED)).isFalse();
        assertThat(MessageStatus.QUEUED.canTransitionTo(MessageStatus.SENT)).isFalse();
        assertThat(MessageStatus.CREATED.canTransitionTo(null)).isFalse();
    }

    @Test
    void terminalStatesHaveNoOutgoingTransitions() {
        for (MessageStatus terminal : List.of(MessageStatus.SENT, MessageStatus.FAILED)) {
            assertThat(terminal.isTerminal()).isTrue();
            for (MessageStatus next : MessageStatus.values()) {
                assertThat(terminal.canTransitionTo(next))
                        .as("%s -> %s 应为非法", terminal, next)
                        .isFalse();
            }
        }
    }

    @Test
    void nonTerminalStatesAreNotTerminal() {
        assertThat(MessageStatus.CREATED.isTerminal()).isFalse();
        assertThat(MessageStatus.QUEUED.isTerminal()).isFalse();
        assertThat(MessageStatus.PROCESSING.isTerminal()).isFalse();
    }

    @Test
    void resolvesValueRoundTrip() {
        assertThat(MessageStatus.fromValue("created")).isEqualTo(MessageStatus.CREATED);
        assertThat(MessageStatus.fromValue("sent")).isEqualTo(MessageStatus.SENT);
        assertThat(MessageStatus.fromValue("unknown")).isNull();
        assertThat(MessageStatus.fromValue(null)).isNull();
    }
}
