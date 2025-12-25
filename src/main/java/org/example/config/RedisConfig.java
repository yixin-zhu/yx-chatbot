package org.example.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class RedisConfig {

    @Bean
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        // 将 Key 设置为普通的 String 这样在 Redis 里看到的键就是明文
        template.setKeySerializer(new StringRedisSerializer());
        // 将 Value 序列化为 JSON 格式。它会自动把 Java 对象转成 JSON 字符串存入 Redis，读取时再自动转回来
        template.setValueSerializer(new GenericJackson2JsonRedisSerializer());
        // 加入没有上面两行代码，那么类需要实现Serializable接口，否则会报错
        return template;
    }
}