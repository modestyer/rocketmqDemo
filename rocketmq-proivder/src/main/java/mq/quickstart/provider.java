package mq.quickstart;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * @author liuf
 * @create 2019-01-13 15:11
 */
public class provider {

    public static void main(String[] args) throws Exception {

        DefaultMQProducer producer = new DefaultMQProducer("test_producer");

//        producer.setNamesrvAddr("192.168.49.131:9876;192.168.49.132:9876");

        producer.setNamesrvAddr("192.168.79.130:9876");
        producer.setRetryTimesWhenSendFailed(10);
        producer.setVipChannelEnabled(false);
        producer.start();

        for (int i=0;i<10;i++){
            Message msg = new Message("TopicProducerTest",
                    "testTags",
                    ("Hello RocketMq"+i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult result = producer.send(msg,1000);
            System.out.printf("%s%n",result);
        }

        producer.shutdown();
    }
}




