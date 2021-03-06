package com.tansun.jlogstash.outputs;

import com.alibaba.fastjson.JSONObject;
import com.tansun.jlogstash.utils.ConditionUtils;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Dave on 2017/11/13.
 *
 * version 1.0 2017/11/13
 * autor zxd
 */
public class RocketMQ extends BaseOutput {

  private Logger logger = LoggerFactory.getLogger(RocketMQ.class);

  //指定生产主题
  private static String topic;

  private static String tags;

  //指定MQ的地址格式10.1.54.121:9876;10.1.54.122:9876
  private static String namesrvAddr;

  //producer group name
  private static String producerGroup;

  //QueueNums
  private static int topicQueueNums;

  //condition
  private static String condition;

  private DefaultMQProducer defaultMQProducer;

  public RocketMQ(Map config) {
    super(config);
  }

  @Override
  public void prepare() {
    try {
      defaultMQProducer = new DefaultMQProducer();
      defaultMQProducer.setProducerGroup(producerGroup);
      defaultMQProducer.setInstanceName(Thread.currentThread().getId()+toString());
      defaultMQProducer.setNamesrvAddr(namesrvAddr);
      if(!StringUtils.isEmpty(topicQueueNums + "")) {
        defaultMQProducer.setDefaultTopicQueueNums(topicQueueNums);
      }
      defaultMQProducer.start();
    } catch (MQClientException e) {
      e.printStackTrace();
      logger.error("RocketMq initialization error",e);
    }
  }

  @Override
  protected void emit(Map event) {
    if(!StringUtils.isEmpty(condition)) {
      if(!ConditionUtils.isTrue(event,condition)) {
        return;
      }
    }
    try {
      Message msg = new Message();
      msg.setTopic(topic);
      if(!StringUtils.isEmpty(tags)) {
        msg.setTags(tags);
      }
      msg.setBody(JSONObject.toJSONBytes(event));
      defaultMQProducer.send(msg);
    } catch (MQClientException e) {
      e.printStackTrace();
      logger.error("RocketMq sendmsg error",e);
    } catch (RemotingException e) {
      e.printStackTrace();
      logger.error("RocketMq sendmsg error",e);
    } catch (MQBrokerException e) {
      e.printStackTrace();
      logger.error("RocketMq sendmsg error",e);
    } catch (InterruptedException e) {
      e.printStackTrace();
      logger.error("RocketMq sendmsg error",e);
    }
  }

}
