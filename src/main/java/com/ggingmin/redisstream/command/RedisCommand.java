package com.ggingmin.redisstream.command;

import ch.qos.logback.core.status.Status;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandKeyword;
import io.lettuce.core.protocol.CommandType;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;

@Component
@RequiredArgsConstructor
public class RedisCommand {

    private final RedisTemplate<String, Object> redisTemplate;

    public Object getRedisValue(String key, String field) {
        return this.redisTemplate.opsForHash().get(key, field);
    }

    public long increaseRedisValue(String key, String field) {
        return this.redisTemplate.opsForHash().increment(key, field, 1);
    }

    public void ackStream(String consumerGroup, MapRecord<String, Object, Object> message) {
        this.redisTemplate.opsForStream().acknowledge(consumerGroup, message);
    }

    public void claimStream(PendingMessage pendingMessage, String consumer) {
        RedisAsyncCommands commands = (RedisAsyncCommands) this.redisTemplate
                .getConnectionFactory()
                .getConnection()
                .getNativeConnection();

        CommandArgs<String, String> commandArgs = new CommandArgs<>(StringCodec.UTF8)
                .add(pendingMessage.getIdAsString())
                .add(pendingMessage.getGroupName())
                .add(consumer)
                .add("");

        commands.dispatch(CommandType.XCLAIM, new StatusOutput(StringCodec.UTF8), commandArgs);
    }

    public MapRecord<String, Object, Object> findStreamMessageById(String streamKey, String id) {
        List<MapRecord<String, Object, Object>> mapRecordList = this.findStreamMessageByRange(streamKey, id, id);
        if (mapRecordList.isEmpty())
            return null;
        return mapRecordList.get(0);
    }

    public List<MapRecord<String, Object, Object>> findStreamMessageByRange(String streamKey, String startId, String endId) {
        return this.redisTemplate.opsForStream().range(streamKey, Range.closed(startId, endId));
    }

    public void createStreamConsumerGroup(String streamKey, String consumerGroup) {
        if (Boolean.FALSE.equals(this.redisTemplate.hasKey(streamKey))) {
            RedisAsyncCommands commands = (RedisAsyncCommands) this.redisTemplate
                    .getConnectionFactory()
                    .getConnection()
                    .getNativeConnection();

            CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
                    .add(CommandKeyword.CREATE)
                    .add(streamKey)
                    .add(consumerGroup)
                    .add("0")
                    .add("MKSTREAM");

            commands.dispatch(CommandType.XGROUP, new StatusOutput(StringCodec.UTF8), args);
        } else {
            if (!isStreamConsumerGroupExist(streamKey, consumerGroup)) {
                this.redisTemplate.opsForStream()
                        .createGroup(streamKey, ReadOffset.from("0"), consumerGroup);
            }
        }
    }

    public PendingMessages findStreamPendingMessage(String streamKey, String consumerGroup, String consumer) {
        return this.redisTemplate.opsForStream()
                .pending(streamKey, Consumer.from(consumerGroup, consumer), Range.unbounded(), 100L);
    }

    public boolean isStreamConsumerGroupExist(String streamKey, String consumerGroup) {
        Iterator<StreamInfo.XInfoGroup> iterator = this.redisTemplate
                .opsForStream()
                .groups(streamKey)
                .stream().iterator();

        while (iterator.hasNext()) {
            StreamInfo.XInfoGroup xInfoGroup = iterator.next();
            if (xInfoGroup.groupName().equals(consumerGroup)) {
                return true;
            }
        }
        return false;
    }

    public StreamMessageListenerContainer createStreamMessageListenerContainer() {
        return StreamMessageListenerContainer.create(
                this.redisTemplate.getConnectionFactory(),
                StreamMessageListenerContainer
                        .StreamMessageListenerContainerOptions.builder()
                        .hashKeySerializer(new StringRedisSerializer())
                        .hashValueSerializer(new StringRedisSerializer())
                        .pollTimeout(Duration.ofMillis(30))
                        .build()
        );
    }
}
