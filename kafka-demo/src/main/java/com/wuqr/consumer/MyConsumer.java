package com.wuqr.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author wql78
 * @title: MyConsumer
 * @description: @TODO
 * @date 2021-11-09 20:35:47
 */
public class MyConsumer {
    public static void main(String[] args) {
        // 1.创建消费者配置信息
        Properties properties = new Properties();
        // 2. 给配置信息赋值
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092"); // 连接的集群信息
        // 打开自动提交offset 如果你关闭自动提交，同时不手动提交，offset就不会改变，你每次重新启动消费者，都是从你第一次开始消费的地方进行消费
        // 消费者内存中会维护一个offset  下面这个配置只是说会不会定期把消费者内存中的这个offset写回到kafka服务端去，写回去了
        // 你当前消费者挂了，下次就能继续消费，没写回去，下次还是从这个消费者本次刚开始消费的地方进行消费，因为offset在消费者内存，并没有同步给kafka
        // kafka本地的offset 只是你当前消费者启动的时候去拉取一次，如果你每次消费消息都去拉取，乖乖的，太麻烦了。
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        // 自动提交offset延迟毫秒数 1秒钟提交一次offset
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        // key-value反序列化类
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        // 消費者組，在控制台消费者，默认会给你随机分配一个数字为名的消费者组，这里不会
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "bigdata1");
        /**
         * 两种情况，一种是你当前消费者组没有消费过这个主题，offset可以重置、从头开始消费。
         * 另外一种情况是数据被删除了，比如你当前消费者所在的消费者组消费偏移量到10，挂了，8天过后来消费，数据只保留一个礼拜，前1000条被删除了，现在最小的都是1001了，你拿着这个10来消费显然不行。
         * earliest 最早的、latest最新的，默认值是latest
         * 指定earliest 必须满足上面两种情况，才会生效 从当前主题第一条消息开始消费
         * 因为你已经消费过主题，已经有一个offset存在，如果没换消费者组，你已经消费过一次，earliest不会生效。
         * 或者说你的offset已经小于当前第一条数据的offset，earliest才会生效。默认就是latest从最新的数据开始消费。
         * 如何重新消费某一个主题的数据，消费者换一个消费者组，同时下面这个属性设置为earliest
         */
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");// 重置消费者的offset，从主题中最早的数据开始消费。
        // 也就是从主题中现有保存的第一条数据开始消费

        // 创建消费者  参数和生产者类似
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // 订阅主题 当前这个消费者可以订阅多个主题，所以是一个List
        // third这个主题是不存在的，可以i订阅一个不存在的主题，也不会影响消费其他正经存在的主题
        // 生产者指定的主题如果不存在会自己创造，不然没地方存你发送的消息，
        // 消费者你指定不存在的主题，消费不到东西就是了，但是不会报错，如果加入log4j会给警告
        consumer.subscribe(Arrays.asList("first", "third"));
        // 死循环去拉取消息，不然一次没拉取到，JVM就自动关闭了  开启消费者，使用MyProducer去发送就可以了
        while (true) {
            // kafka是基于消费者拉取的发布订阅模式，一直在维护这个长轮询，一旦没有数据的时候这个长轮询是浪费资源。如果没拉取到数据，我延迟时间要长一点
            // 所以拉取方法要有一个延迟时间参数
            // 批量拉取，一次可以拉取多条数据 在Records对象里
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
            // 解析并打印consumerRecords  ConsumerRecords是ConsumerRecord的迭代类型
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.key() +"--" + consumerRecord.value());
            }
        }

        // 关闭连接
//        consumer.close(); 死循环了，这个代码就多余了
    }
}
