package com.wuqr.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author wql78
 * @title: CounterInterceptor 计数拦截器，计数，统计成功失败的条数
 * @description: 计数， 是在消息发送成功回调onAcknowledgement进行累加，最终输出，在close输出，所以要一个静态的变量记录
 * CounterInterceptor TimeInterceptor形成了一个拦截器链。
 * @date 2021-11-11 20:19:04
 */
public class CounterInterceptor implements ProducerInterceptor<String, String> {
    int success;
    int error;
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;// 这里如果返回null 等于写了一个过滤拦截器，把所有消息设置为null
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if(metadata != null) {// 说明这条数据发送是成功 的，
            success++;
        } else {
            error++;
        }
    }

    @Override
    public void close() {
        System.out.println("success: " + success);
        System.out.println("error: " + error);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
