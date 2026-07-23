package com.minisql.result.format;

import com.minisql.result.ExecutionResult;

/**
 * 定义 SQL 执行结果到字节输出的转换，便于不同客户端实现自定义格式。
 */
public interface ResultFormatter {

    byte[] format(ExecutionResult result);
}
