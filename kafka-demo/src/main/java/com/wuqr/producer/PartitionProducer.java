package com.wuqr.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author wql78
 * @title: PartitionProducer
 * @description: 使用自定义分区器的生产者
 * @date 2021-11-07 21:21:08
 */
public class PartitionProducer {
    // 如何在发送的时候指定分区器，猜想就三个地方嘛，一个是调用send方法的时候指定分区器，一个是创建生产者对象的时候指定分区器，
    // 一个是在声明Properties配置信息的时候指定，前两者都不行了，那肯定就是第三者了
    public static void main(String[] args) {
        // 1. 创建配置信息
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092");
        // 记住类名，StringSerializer 是kafka包下的，然后去找就行了
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // 自定义组件 如果要在配置里面使用，肯定要声明全类名，程序运行的时候再通过反射的方式用这个全类名去创建对象
        // 那么这个配置的key是什么。一定是和partition有关的，那就用这个ProducerConfig去搜Partition相关的配置咯
        // 这里使用的自定义的分区器，返回的就是1号分区，那么所有数据都会往1号分区里面生产了。
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.wuqr.partitioner.MyPartitioner");
        // 如果是在创建生产者对象，或者send的时候，肯定就是创建自定义组件的一个对象传进去了
        // 2. 创建生产者对象
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(properties);
        List<String> list = new ArrayList<>();
        list.add("a");
        list.add("b");
        list.add("c");
        // 3. 发送数据 直接new 接口 就是匿名实现类的匿名对象
        for (int i = 0; i < 10; i++) {
            // 分区号0之后的atguigu是消息的key
            // 如果不带分区号，就给一个key 是按照key的哈希值去算分区号
            kafkaProducer.send(new ProducerRecord<>("first",  "atguigu", "atguigu--" + i), (metadata, exception) -> {
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
