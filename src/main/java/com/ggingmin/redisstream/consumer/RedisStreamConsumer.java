package com.ggingmin.redisstream.consumer;

import com.ggingmin.redisstream.command.RedisCommand;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
@RequiredArgsConstructor
public class RedisStreamConsumer implements StreamListener<String, MapRecord<String, Object, Object>>, InitializingBean, DisposableBean {

    private StreamMessageListenerContainer<String, MapRecord<String, Object, Object>> streamMessageListenerContainer;
    private Subscription subscription;
    private String streamKey;
    private String consumerGroup;
    private String consumer;

    private final RedisCommand redisCommand;


    @Override
    public void onMessage(MapRecord<String, Object, Object> message) {

        System.out.println(message.getId());
        System.out.println(message.getStream());
        System.out.println(message.getValue());

        this.redisCommand.ackStream(streamKey, message);
    }

    @Override
    public void destroy() throws Exception {
        if (this.subscription != null) {
            this.subscription.cancel();
        }

        if (this.streamMessageListenerContainer != null) {
            this.streamMessageListenerContainer.stop();
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.streamKey = "redisstream2";
        this.consumerGroup = "consumeGroup";
        this.consumer = "consumer";

        this.redisCommand.createStreamConsumerGroup(streamKey, consumerGroup);

        this.streamMessageListenerContainer = this.redisCommand.createStreamMessageListenerContainer();

        this.subscription = this.streamMessageListenerContainer.receive(
                Consumer.from(this.consumerGroup, consumer),
                StreamOffset.create(streamKey, ReadOffset.lastConsumed()),
                this
        );

        this.subscription.await(Duration.ofSeconds(1));
        this.streamMessageListenerContainer.start();
    }
}
