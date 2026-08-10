package com.cex.core.engine;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 项目骨架构建级 smoke test。
 *
 * <p>验证运行时至少支持 Java 11，保证纯 JDK 工程的编译基础。</p>
 */
class ProjectSkeletonTest {

    /** 验证测试运行时的 Java 主版本。 */
    @Test
    void usesTheExpectedJavaRuntime() {
        assertTrue(Runtime.version().feature() >= 11);
    }
}
