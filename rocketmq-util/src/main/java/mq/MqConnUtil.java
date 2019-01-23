package mq;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;

/**
 * @author liuf
 * @create 2019-01-13 22:18
 */
public class MqConnUtil {

    private final static String NAME_SRV="192.168.49.131:9876;192.168.49.132:9876";


    public static DefaultMQPushConsumer getConsumer(String consumerGroup){
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);

        consumer.setNamesrvAddr(NAME_SRV);
        consumer.setVipChannelEnabled(false);
        return consumer;
    }

    public static DefaultMQProducer getProvider(String producerGroup){
        DefaultMQProducer producer = new DefaultMQProducer(producerGroup);

        producer.setNamesrvAddr(NAME_SRV);
        producer.setVipChannelEnabled(false);
        return producer;
    }

}
