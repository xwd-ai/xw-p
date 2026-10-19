package com.xwp.engine.domain;

/**
 * 隔离环境枚举，与 Python 侧 {@code api.app.models.entities.Environment} 对齐。
 */
public enum Environment {
    DEVELOPMENT("development"),
    PRODUCTION("production");

    private final String value;

    Environment(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    /** 按存储值解析环境，未知值返回 {@code null}。 */
    public static Environment fromValue(String value) {
        if (value == null) {
            return null;
        }
        for (Environment e : values()) {
            if (e.value.equals(value)) {
                return e;
            }
        }
        return null;
    }
}
