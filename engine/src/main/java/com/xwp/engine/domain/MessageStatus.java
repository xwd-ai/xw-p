package com.xwp.engine.domain;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * 消息状态机状态定义，与 Python 侧 {@code Message.Status} 对齐。
 *
 * <p>合法转移路径：</p>
 * <pre>
 *   created → queued → processing → sent
 *                                 ↘ failed
 * </pre>
 *
 * <p>此外允许 {@code processing → queued}（投递前回滚重试），
 * 以及 {@code queued → failed} / {@code processing → failed}（投递失败终态）。
 * 状态转移校验由 {@link #canTransitionTo(MessageStatus)} 承担。</p>
 */
public enum MessageStatus {
    CREATED("created"),
    QUEUED("queued"),
    PROCESSING("processing"),
    SENT("sent"),
    FAILED("failed");

    private final String value;

    /** 每个状态允许迁入的下一状态集合。 */
    private static final Map<MessageStatus, Set<MessageStatus>> TRANSITIONS = Map.of(
            CREATED, EnumSet.of(QUEUED, FAILED),
            QUEUED, EnumSet.of(PROCESSING, FAILED),
            PROCESSING, EnumSet.of(SENT, FAILED, QUEUED),
            SENT, EnumSet.noneOf(MessageStatus.class),
            FAILED, EnumSet.noneOf(MessageStatus.class));

    MessageStatus(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    /** 按存储值解析状态，未知值返回 {@code null}。 */
    public static MessageStatus fromValue(String value) {
        if (value == null) {
            return null;
        }
        for (MessageStatus s : values()) {
            if (s.value.equals(value)) {
                return s;
            }
        }
        return null;
    }

    /** 是否为终态（sent / failed），终态不再允许任何转移。 */
    public boolean isTerminal() {
        return this == SENT || this == FAILED;
    }

    /**
     * 判断从当前状态能否合法转移到 {@code target}。
     *
     * @param target 目标状态，{@code null} 视为非法
     * @return 合法返回 {@code true}
     */
    public boolean canTransitionTo(MessageStatus target) {
        if (target == null) {
            return false;
        }
        return TRANSITIONS.getOrDefault(this, EnumSet.noneOf(MessageStatus.class))
                .contains(target);
    }
}
