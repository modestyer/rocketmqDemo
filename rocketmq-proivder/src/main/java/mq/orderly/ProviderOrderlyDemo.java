package mq.orderly;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

public class ProviderOrderlyDemo {

    public static void main(String[] args) {

        DefaultMQProducer producer = new DefaultMQProducer("order_producer_demo");

        producer.setNamesrvAddr("192.168.79.130:9876");


        try {
            producer.start();
            int m1=5;
            for (int i = 0; i < m1; i++) {
                String body = "order_1_"+i;
                Message message = new Message("order_topic","order_tag",body.getBytes());
                SendResult sendResult =producer.send(message, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                        Integer id = (Integer) o;
                        System.out.println("id："+id);
                        return list.get(id);
                    }
                },0);
                System.out.println(sendResult + ", body:" + body);
            }


            /*for (int i = 0; i < m1; i++) {
                String body = "order_2_"+i;
                Message message = new Message("order_topic","order_tag2",body.getBytes());
                SendResult sendResult = producer.send(message, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                        Integer id = (Integer) o;

                        return list.get(id);
                    }
                },1);
                System.out.println(sendResult + ", body:" + body);
            }

            for (int i = 0; i < m1; i++) {
                String body = "order_5_"+i;
                Message message = new Message("order_topic","order_tag1",body.getBytes());
                SendResult sendResult = producer.send(message, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                        Integer id = (Integer) o;

                        return list.get(id);
                    }
                },0);
                System.out.println(sendResult + ", body:" + body);
            }*/
            producer.shutdown();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
