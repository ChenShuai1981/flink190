package org.apache.flink.cep;

import org.apache.flink.cep.pattern.Pattern;

import java.io.Serializable;
import java.util.Map;

/**
 * 增加InjectionPatternFunction函数，在inject()方法中监听Rules变化，若需要更新，则通过Groovy加载Pattern类进行动态注入
 * 若使用DB轮询的方式来做Rules的管理，需要设置轮询周期。
 * 若用ZK+DB来做Rules的管理，不需要设置轮询周期，ZK监听Rules的变化，从DB中获取Rules
 */
public interface InjectionPatternFunction extends Serializable {

    // 初始化外部连接
    void initialize() throws Exception;

    // 规则动态注入
    Map<String, Pattern> inject() throws Exception;

    // 轮询周期 （轮询模式使用，监听模式不需要）
    long getPeriod();

}
