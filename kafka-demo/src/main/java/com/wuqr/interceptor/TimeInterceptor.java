package com.wuqr.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author wql78
 * @title: TimeInterceptor 时间拦截器，拦截生产者发布的消息，给消息加上时间戳
 * @description: 拦截器一定在生产者这边，没有说消费者还定义拦截器
 * @date 2021-11-11 20:12:48
 */
public class TimeInterceptor implements ProducerInterceptor<String, String> {
    // 拦截器可以修改发送的消息。 拦截所有消息进行统一处理。
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
//        1. 取出数据
        String value = record.value();
        // 2. 创建一个新的record对象并返回。record没有设置value的值，我们只能构建一个新的record对象。
        long timeMillis = System.currentTimeMillis();
        String newValue = timeMillis + "," + value;
        ProducerRecord<String, String> newRecord = new ProducerRecord<>(record.topic(), record.partition(), record.key(), timeMillis + "," + record.value());
        return newRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
