package com.xwp.engine.domain;

import java.util.Arrays;
import java.util.List;

/**
 * 通知投递渠道枚举，与 Python 侧 {@code api.app.models.channel.Channel} 对齐。
 *
 * <p>值即存储/传输标识（snake_case）。{@link #DELAY} 与 {@link #DIGEST} 属于
 * 编排步骤而非投递渠道，由 {@link #deliveryChannels()} 排除。</p>
 */
public enum Channel {
    /** 应用内收件箱 */
    IN_APP("in_app"),
    /** 邮件 */
    EMAIL("email"),
    /** 短信 */
    SMS("sms"),
    /** 移动推送 */
    PUSH("push"),
    /** 聊天工具（webhook） */
    CHAT("chat"),
    /** 定时延迟（编排步骤，非投递渠道） */
    DELAY("delay"),
    /** 摘要聚合（编排步骤，非投递渠道） */
    DIGEST("digest");

    private final String value;

    Channel(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    /** 按存储/传输标识解析渠道，未知值返回 {@code null}。 */
    public static Channel fromValue(String value) {
        if (value == null) {
            return null;
        }
        for (Channel c : values()) {
            if (c.value.equals(value)) {
                return c;
            }
        }
        return null;
    }

    /** 返回真正执行投递的渠道（排除 delay/digest 编排步骤）。 */
    public static List<Channel> deliveryChannels() {
        return Arrays.stream(values())
                .filter(c -> c != DELAY && c != DIGEST)
                .toList();
    }
}
