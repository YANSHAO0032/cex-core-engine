package com.cex.core.order;

import java.util.Objects;

/**
 * 受规范校验的资产标识值对象。
 *
 * <p>核心能力：以不可变大写代码唯一标识可交易资产。</p>
 * <p>线程安全：记录及其字符串组件均不可变，可在线程间安全共享。</p>
 * <p>使用限制：仅表达资产代码，不承载精度、链网络或展示名称。</p>
 *
 * @param value 长度为 2 至 16 的大写字母或数字资产代码
 */
public record AssetId(String value) {

    /**
     * 创建并校验资产标识。
     *
     * @param value 长度为 2 至 16 的大写字母或数字资产代码
     * @throws NullPointerException 当资产代码为 {@code null} 时抛出
     * @throws IllegalArgumentException 当资产代码不符合规范时抛出
     */
    public AssetId {
        Objects.requireNonNull(value, "value");
        if (!value.matches("[A-Z0-9]{2,16}")) {
            throw new IllegalArgumentException("asset code must match [A-Z0-9]{2,16}");
        }
    }
}
