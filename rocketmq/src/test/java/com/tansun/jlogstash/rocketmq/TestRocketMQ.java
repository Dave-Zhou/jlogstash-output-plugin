package com.tansun.jlogstash.rocketmq;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * Created by Dave on 2017/11/14.
 *
 * @version 1.0 2017/11/14
 * @autor zxd
 */
public class TestRocketMQ {
  public static void main(String[] args) throws MQClientException {
    DefaultMQPushConsumer mqPushConsumer = new DefaultMQPushConsumer();
    mqPushConsumer.setNamesrvAddr("127.0.0.1:9876");
    mqPushConsumer.setConsumerGroup("testConsumer");
    mqPushConsumer.setVipChannelEnabled(false);
    mqPushConsumer.subscribe("JIGTopic","");
    mqPushConsumer.registerMessageListener(new MessageListenerOrderly() {
      @Override
      public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list,
          ConsumeOrderlyContext consumeOrderlyContext) {
        for(MessageExt msg : list) {
          System.out.println(new String(msg.getBody()));
        }
        return ConsumeOrderlyStatus.SUCCESS;
      }
    });
    mqPushConsumer.start();
    System.out.println("启动了");
  }
}
