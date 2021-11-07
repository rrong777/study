package com.wuqr.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author wql78
 * @title: MyProducer
 * @description: @TODO
 * @date 2021-11-07 19:34:29
 */
public class MyProducer {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        // 1. 先创建kafka生产者配置信息 你在命令行下 生产 也要--zookeeper --topic这些 在代码里面也要这些参数
        Properties props = new Properties();// properties 也是kv键值对形式的
        // 2. 指定连接的kafka集群，指定一个节点的地址即可，也可以指定多个，在命令行中是--broker-list
        // 你这里node1:9092这么写了，就表示你host里面有配置
//        props.put("bootstrap.servers", "node1:9092");
        // ProducerConfig这个里面就是记录一些常量，你根本不用去记忆这些配置的key
        // 有三个配置类 ProducerConfig ConsumerConfig CommonClientConfig
        // CommonClientConfig是消费者和生产者都有的配置
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

        // 9. 创建生产者对象 你要在Java代码中使用其他组件，你首先都要先new一个客户端 连接上组件服务端把
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);// 带泛型的，
        // send方法有一个重载的，给你返回一个callback 返回你发送这条数据在kafka中的情况，
        // 在我Kafka里面，哪个主题，哪个分区，offset是多少，给你带回来，因为你发出去的时候 ，你生产者还不知道offset

        // 10. 发送数据
        for (int i = 0; i < 10; i++) {
            // 没有.get()的时候就是异步发送消息，有.get()的时候就变成同步了，因为send方法返回的是一个Future对象，这个对象的get方法会阻塞之前的一个
            // 线程，这个同步是我们人为造成的，但是这种发送效率太低了。同步发送有一种特定场景会用到，kafka是分区内有序性，如果我想保证全局有序性
            // 就需要用同步发送了。 异步发送，你就算只有一个分区都不能保证有序性，你先发123，再发456  456接收到了，123不好意思没接收到，再重新发送
            // 没有顺序了
            producer.send(new ProducerRecord<>("first", "atguigu--" + i)).get();

        }
        // 循环10次一毫秒都不需要， 如果你没有close方法，执行完了之后线程就结束了，根本不会发送消息
        // 消息是发不出去的。你睡眠100ms，循环结束，加上睡眠的过程中，线程还没结束，已经到了1ms，到了 linger.ms设置的时间，就会把你的消息发送出去。
        // 如果没睡眠100ms，也没调用close，那么你的消息还没发出去（10次循环结束后），线程就结束了
//         Thread.sleep(100);

        // 11. 关闭资源 close方法就像一个钩子函数一样，你调用了close 把你内存的东西都发送出去。虽然没达到1ms也没有16K的数据。就会去把内存中的东西清除，
        producer.close();; // close方法除了关闭资源 可能还要做很多事情哦 ，比如说这里

        // 可以先在kafka服务端开启一个控制台消费者做测试,然后启动这个main方法发送
        // bin/kafka-console-consumer.sh --zookeeper node1:2181 --topic first
        // 如下是控制台消费者的消费情况，生产者是批量发送，轮询的方式，0进入1号分区，1进入2号分区，2进入0号分区……
        // 258在0号分区，0369在1号分区，147在2号分区，数据分别一次性写入自己的分区，消费者先看A有数据了，有可能先消费0号分区，有可能先消费1号分区
        // 恰好0号分区数据来了，先把0号分区数据都拿出来，然后再去拿1号分区。 多发送几次结果出来的舒徐可能不同，哪个分区先有数据就先消费哪个分区的。
        // 批量发送，所以你看到消费的时候也是批量消费，一个分区一个分区的去消费，所以出来的结果看到没有按照顺序，
        // 轮询的放到RecordAccumulator里
        /**
         *         atguigu--2
         *         atguigu--5
         *         atguigu--8
         *         atguigu--0
         *         atguigu--3
         *         atguigu--6
         *         atguigu--9
         *         atguigu--1
         *         atguigu--4
         *         atguigu--7
         */

    }
}
