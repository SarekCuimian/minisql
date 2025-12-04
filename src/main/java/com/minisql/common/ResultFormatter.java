package com.minisql.common;

/**
 * 定义 SQL 执行结果到字节输出的转换，便于不同客户端实现自定义格式。
 */
public interface ResultFormatter {

    byte[] format(ExecResult result);
}
