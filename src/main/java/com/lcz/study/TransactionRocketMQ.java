package com.lcz.study;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.jupiter.api.Test;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TransactionRocketMQ {

    private final static String nameSrv = "rocketmqA:9876;rocketmqB:9876;rocketmqC:9876";
    private final static int maxPoolSize = Runtime.getRuntime().availableProcessors();
    private final static String topic = "transaction_topic";

    @Test
    public void producerTransaction() throws Exception {
        TransactionMQProducer producer = new TransactionMQProducer("transaction_producer");
        producer.setNamesrvAddr(nameSrv);

        producer.setTransactionListener(new TransactionListener() {

            // 业务需要的参数：
            // 1. message的body，带来的问题：网络带宽、body编码污染
            // 2. 通过userProperty，带来的问题：网络带宽
            // 3. arg，local的
            @Override
            public LocalTransactionState executeLocalTransaction(Message message, Object o) {
                // send half ok 之后执行本地事务
//                String action = new String(message.getBody());
//                String action = message.getProperty("action");
                String action = (String) o;
                String transactionId = message.getTransactionId();
                System.out.println("transactionId: " + transactionId);
                // 状态有两个：1.rocketmq的half message，驱动回查producer  2.service应该是无状态的，应该把transactionId随着本地事务的执行写入事件状态表
                switch (action) {
                    case "0":
                        // 模拟异步调用
                        System.out.println(Thread.currentThread().getName() + " send half: async api call...action 0");
                        return LocalTransactionState.UNKNOW; // rocketmq会回调check，在check中通过transactionId在本地数据表中查状态
                    case "1":
                        System.out.println(Thread.currentThread().getName() + " send half: transaction failed...action 1");
                        /*
                        transaction.begin
                        .....
                        throw
                        transaction.rollback
                         */
                        return LocalTransactionState.ROLLBACK_MESSAGE; // consumer是消费不到rollback的message的
                    case "2":
                        System.out.println(Thread.currentThread().getName() + " send half: transaction success...action 2");
                        /*
                        transaction.begin
                        .....
                        transaction.commit
                         */
                        try {
                            Thread.sleep(10000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        return LocalTransactionState.COMMIT_MESSAGE; // consumer是肯定消费到message的
                }

                return null;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
                // callback check
                String action = messageExt.getProperty("action");
                int times = producer.getRetryTimesWhenSendFailed();
                switch (action) {
                    case "0":
                        System.out.println(Thread.currentThread().getName() + " check: action 0 times " + times);
                        // 模拟通过次数来确认消息
                        if (times < 2) {
                            return LocalTransactionState.UNKNOW;
                        } else {
                            return LocalTransactionState.COMMIT_MESSAGE;
                        }
                    case "1":
                        System.out.println(Thread.currentThread().getName() + " check: action 1 rollback times " + times);
                        // 为什么rollback了还会check，因为存在时间窗口。这里应该继续观察事务表
                        return LocalTransactionState.UNKNOW;
                    case "2":
                        System.out.println(Thread.currentThread().getName() + " check: action 2 commit times " + times);
                        // 为什么commit了还会check，因为存在时间窗口。这里应该继续观察事务表
                        return LocalTransactionState.UNKNOW;
                }


                return null;
            }
        });

        producer.setExecutorService(new ThreadPoolExecutor(
                1,
                maxPoolSize,
                2000,
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(),
                r -> new Thread(r, "transaction thread")));

        producer.start();

        for (int i = 0; i < 10; i++) {
            Message message = new Message(topic, "tagM", "key_" + i, (i % 3 + "").getBytes());
            message.putUserProperty("action", i % 3 + "");

            producer.sendMessageInTransaction(message, i % 3 + "");
        }

        System.in.read();
        producer.shutdown();
    }

    @Test
    public void consumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("transaction_consumer");
        consumer.setNamesrvAddr(nameSrv);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        // 订阅
        consumer.subscribe(topic, "*");
        // 注册监听器
        consumer.registerMessageListener((MessageListenerConcurrently) (list, context) -> {
            list.forEach(messageExt -> System.out.println(new String(messageExt.getBody())));

            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        consumer.start();
        System.in.read();
    }

}
