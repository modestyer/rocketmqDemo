package mq.quickstart;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author liuf
 * @create 2019-01-13 15:21
 */
public class consumer {
    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test_consumer");

//        consumer.setNamesrvAddr("192.168.49.131:9876;192.168.49.132:9876");
        consumer.setNamesrvAddr("192.168.79.130:9876");
        consumer.setVipChannelEnabled(false);
        consumer.setConsumeThreadMax(20);
        consumer.setConsumeMessageBatchMaxSize(10);
        consumer.subscribe("TopicProducerTest","*");

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                System.out.println("消息条数："+msgs.size());
                try {
                    for(MessageExt messageExt : msgs){
                        String topic = messageExt.getTopic();
                        String msgbody = new String(messageExt.getBody(),"utf-8");
                        String tags = messageExt.getTags();
                        System.out.println("收到消息："+"   topic:"+topic+"   ,tags:"+tags+"  ,msg:"+msgbody);
                        System.out.println("线程为："+Thread.currentThread().getName());
                        /*if("Hello RocketMq4".equals(msgbody)){
                            System.out.println("=====失败消息开始=====");
                            System.out.println(msgbody);
                            System.out.println(messageExt);
                            System.out.println("=====失败消息结束=====");
                            //异常
                            int a=1/0;
                        }*/
                    }

                }catch (Exception e){
                    e.printStackTrace();
                    //隔一段时间继续发送  重试
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();

        System.out.printf("Consumer Started.%n");
    }
}
