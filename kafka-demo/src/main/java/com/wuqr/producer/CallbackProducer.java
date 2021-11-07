package com.wuqr.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @author wql78
 * @title: CallbackProducer
 * @description: @TODO
 * @date 2021-11-07 20:41:33
 */
public class CallbackProducer {
    public static void main(String[] args) {
        // 1. 创建配置信息
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092");
        // 记住类名，StringSerializer 是kafka包下的，然后去找就行了
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 2. 创建生产者对象
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(properties);

        // 3. 发送数据 直接new 接口 就是匿名实现类的匿名对象
        for (int i = 0; i < 10; i++) {
            kafkaProducer.send(new ProducerRecord<>("first", "atguigu--" + i), (metadata, exception) -> {
                // 成功了返回的就是metadata 失败了返回的就是异常
                // 新创建一个主题，发送消息，可以看到offset是从0开始计算的
                if(exception == null) {
                    System.out.println(metadata.partition() + "--" + metadata.offset());
                } else {
                    exception.printStackTrace();
                }
            });
        }

        // 4. 关闭资源
        kafkaProducer.close();
    }
}
