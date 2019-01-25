package mq.orderly;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class ConsumerOrderlyDemo {

    public static void main(String[] args) {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("order_consumer");

        consumer.setNamesrvAddr("192.168.79.130:9876");

        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        try {
            consumer.subscribe("order_topic", "*");

            consumer.registerMessageListener(new MessageListenerOrderly() {
                @Override
                public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                    consumeOrderlyContext.setAutoCommit(true);
                    Random random = new Random();
                    try {
                        for(MessageExt message : list){

                                String topic = message.getTopic();
                                String body = new String(message.getBody(),"utf-8");
                                String tags = message.getTags();


                            TimeUnit.SECONDS.sleep(random.nextInt(5));
                            System.out.println("接收的消息线程："+Thread.currentThread().getName()+"   topic："+topic+"   tag："+tags+"   body："+body);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
//                            return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                    }
                    return ConsumeOrderlyStatus.SUCCESS;
                }
            });
            consumer.start();
            System.out.println("consumer start....");
        } catch (Exception e) {
            e.printStackTrace();
        }




    }
}

