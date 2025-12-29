package org.example.config;

import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JacksonConfig {

    @Bean
    public Jackson2ObjectMapperBuilderCustomizer jacksonCustomizer() {
        return builder -> {
            // 1. 解决中文不转义为 Unicode 的问题
            builder.featuresToDisable(com.fasterxml.jackson.core.JsonGenerator.Feature.ESCAPE_NON_ASCII);

            // 2. 这里的 builder 默认就会使用 UTF-8，所以不需要再写 StringHttpMessageConverter

            // 3. (顺便建议) 统一日期格式，防止前端解析困难
            builder.simpleDateFormat("yyyy-MM-dd HH:mm:ss");
        };
    }
}
