package mq.tag;

import mq.MqConnUtil;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * @author liuf
 * @create 2019-01-13 22:17
 */
public class ProviderTag {

    public static void main(String[] args) throws  Exception{
        DefaultMQProducer producer = MqConnUtil.getProvider("test_tag_producer");

        producer.start();

        String tag="";
        for(int i=0;i<100;i++){
            /*if(i%2==0){
                tag="TagA";
            }else{
                tag="TagB";
            }*/
            Message message = new Message("Topic1",
                    "Tag1",
                    ("Hello RocketMq Tag-"+i).getBytes("utf-8"));

            SendResult sendResult = producer.send(message);
            System.out.println(sendResult);
        }

        producer.shutdown();
    }
}
