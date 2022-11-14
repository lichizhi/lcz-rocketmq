package com.lcz.study;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class SimpleRocketMQ {

    @Test
    public void producer() throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("ooxx");
        // rocketmqA本地host映射
        producer.setNamesrvAddr("rocketmqA:9876");
        producer.start();

//        CountDownLatch countDownLatch = new CountDownLatch(10);
        // 发送消息
        for (int i = 0; i < 10; i++) {
            Message message = new Message();
            message.setTopic("wula");
            message.setTags("tagA");
            message.setBody(("wula_" + i).getBytes());

            // 1.同步发送
//            SendResult sendResult = producer.send(message);
//            System.out.println(sendResult);

            // 2.异步发送
//            producer.send(message, new SendCallback() {
//                @Override
//                public void onSuccess(SendResult sendResult) {
//                    countDownLatch.countDown();
//                    System.out.println(sendResult);
//                }
//
//                @Override
//                public void onException(Throwable throwable) {
//                    countDownLatch.countDown();
//                    System.out.println(throwable.getMessage());
//                }
//            });

            // 3.oneway
//            producer.sendOneway(message);

            // 4.指定queue
            // 这个场景很少
            MessageQueue mq = new MessageQueue("wula", "rocketmqb", 0);

            SendResult result = producer.send(message, mq);
            System.out.println(result);

        }

//        if (countDownLatch.await(5, TimeUnit.SECONDS))
//            producer.shutdown();
    }

    @Test
    public void consumerPush() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("c_ooxx");
        consumer.setNamesrvAddr("rocketmqA:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        // 订阅
        consumer.subscribe("wula", "*");
        // 注册监听器
        consumer.registerMessageListener((MessageListenerConcurrently) (list, context) -> {
            list.forEach(messageExt -> {
//                    System.out.println(messageExt);
                System.out.println(new String(messageExt.getBody()));
            });

            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        consumer.start();
        System.in.read();

    }

    @Test
    public void admin() throws Exception {

        DefaultMQAdminExt admin = new DefaultMQAdminExt();
        admin.setNamesrvAddr("rocketmqA:9876");
        admin.start();

        TopicList topicList = admin.fetchAllTopicList();
        Set<String> list = topicList.getTopicList();
        System.out.println(list);

        TopicRouteData wula = admin.examineTopicRouteInfo("wula");
        System.out.println(wula);

    }

}
