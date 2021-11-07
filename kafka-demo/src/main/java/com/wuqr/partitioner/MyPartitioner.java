package com.wuqr.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author wql78
 * @title: MyPartitioner
 * @description: 自定义分区器
 * @date 2021-11-07 21:03:54
 * 自定义的组件，要嘛实现一个接口，要嘛继承一个类。
 * Partitioner有一个DefaultPartitioner 默认实现，partition()方法去看源码。可以发现，默认的分区器，会看你send的key空不空，
 * 如果key为空，是产生一个随机数和可用分区数取模，如果可用分区数<=0,那么会返回一个不可用的分区数，send过去会抛异常（不存在可用分区数，就是你给主题分了3个区
 * 结果3个区所在的节点都挂了，类似这种情况）如果key不为空，则用key的哈希模可用分区数
 * 空的话有一个策略，不空的话就是计算key的hash值 然后和topic的分区数量取模，
 * 走到分区器来，一定是你send的时候没有指定分区的，才会有分区器上场
 */
public class MyPartitioner implements Partitioner {
    /**
     * 已经是序列化之后了，所以key是object，还有byte数组，就是序列化后的产物
     * @param topic
     * @param key
     * @param keyBytes
     * @param value
     * @param valueBytes
     * @param cluster
     * @return
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // 方法见名知意的重要性，看框架代码partitionCountForTopic 我就知道这个方法是获得 主题的分区数量，阅读代码非常舒服
        // 具体实现我就不管了。
        //        Integer integer = cluster.partitionCountForTopic(topic);
//        int partition = key.toString().hashCode() % integer;
        Integer partition = 1;
        // 上面怎么写，就看你怎么设计
        return partition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
