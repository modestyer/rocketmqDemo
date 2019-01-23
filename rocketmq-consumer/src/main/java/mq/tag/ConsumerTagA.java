package mq.tag;

import mq.MqConnUtil;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @author liuf
 * @create 2019-01-13 22:17
 */
public class ConsumerTagA {

    public static void main(String[] args) throws Exception{
        DefaultMQPushConsumer consumer = MqConnUtil.getConsumer("Test_consumer_Tag");

        consumer.subscribe("Topic1","Tag1 || Tag2 || Tag3");

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                try {
                    MessageExt msg = list.get(0);
                    String topic = msg.getTopic();
                    String msgbody = new String(msg.getBody(),"utf-8");
                    String tags = msg.getTags();
                    System.out.println("收到消息："+"   topic:"+topic+"   ,tags:"+tags+"  ,msg:"+msgbody);
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();

        System.out.printf("Consumer Started.%n");
    }
}
