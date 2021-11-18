package com.wuqr.interceptor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @author wql78
 * @title: InterceptorProducer
 * @description: 使用拦截器的生产者
 * @date 2021-11-11 20:23:37
 */
public class InterceptorProducer {
    public static void main(String[] args) {
        // 1. 先创建kafka生产者配置信息 你在命令行下 生产 也要--zookeeper --topic这些 在代码里面也要这些参数
        Properties props = new Properties();// properties 也是kv键值对形式的
        // 2. 指定连接的kafka集群，指定一个节点的地址即可，也可以指定多个，在命令行中是--broker-list
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092");
        // 3. ack应答级别，
        props.put("acks", "all");
        // 4. 重试次数
        props.put("retries", 1);
        // 5. 批次大小 一次发送数据的大小 单位字节 这里是16K
        props.put("batch.size", 16384);
        // 6. 等待时间 假设我就只有1条数据，没到16k 等待1ms我还是要发送出去。两个发送条件，满足一个即可
        props.put("linger.ms", 1);
        // 7. RecordAccumulator 缓冲区大小 32M， 上面到了16K 就往这个缓冲区写，  5  6  7 都有默认值 可以都不配。
        props.put("buffer.memory", 33554432);
        // 8. 指定key value的序列化类，
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        // 添加拦截器
        ArrayList<String> interceptors = new ArrayList<>();
        interceptors.add("com.wuqr.interceptor.CounterInterceptor");
        interceptors.add("com.wuqr.interceptor.TimeInterceptor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

            // 9. 创建生产者对象 你要在Java代码中使用其他组件，你首先都要先new一个客户端 连接上组件服务端把
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);// 带泛型的，

        // 10. 发送数据
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("first", "atguigu","atguigu--" + i));

        }
//         Thread.sleep(100);
        // kafka集群上面开启一个控制台消费者做测试：bin/kafka-console-consumer.sh --zookeeper node1:2181 --topic first
        // 或者代码里面开启一个消费者也可以
        // 11. 关闭资源 close方法就像一个钩子函数一样，你调用了close 把你内存的东西都发送出去。虽然没达到1ms也没有16K的数据。就会去把内存中的东西清除，
        producer.close();; // 如果这里注释了，没有调用close 也没有睡眠100ms，拦截器不会触发，拦截器是发送消息的时候触发的，现在都没发 不会触发拦截器
        // 如果是睡眠100ms 没有调用close方法，拦截器里的close也不会调用。
        // 生产者的close方法不止要清空生产者的缓存，将缓存中的消息发送出去，还要把相关的，比如说分区器有close，他会去把分区器的close调用一下
        // 拦截器里面有close 他也会去把拦截器的close调用一下。关闭资源都要放在try catch的finally里面，
    }
}
