package org.example.config;

import org.example.filter.JwtAuthenticationFilter;
import org.example.filter.OrgTagAuthorizationFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;

import java.util.Arrays;
import java.util.Collections;


// @Configuration表示这是一个配置类，里面有Bean定义，Spring容器会扫描并加载它
@Configuration
// @EnableWebSecurity启用自定义的Web安全配置，会自动注册一个名为 SecurityFilterChain 的过滤器链，我们来实现它
@EnableWebSecurity
public class SecurityConfig {
    // 日志记录器，用于记录安全配置的相关信息
    private static final Logger logger = LoggerFactory.getLogger(SecurityConfig.class);
    // 这两个过滤器，被过滤器链调用，用于处理JWT认证和组织标签授权
    @Autowired
    private JwtAuthenticationFilter jwtAuthenticationFilter;
    @Autowired
    private OrgTagAuthorizationFilter orgTagAuthorizationFilter;


     // 该方法主要用于配置应用的安全规则，包括哪些请求需要授权、CSRF保护的启用或禁用、会话管理策略等
    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        try {
            // 禁用CSRF保护
            http.csrf(csrf -> csrf.disable())
                    // 配置请求的授权规则
                    .authorizeHttpRequests(authorize -> authorize
                            // 允许静态资源访问
                            .requestMatchers("/", "/test.html", "/static/test.html", "/static/**", "/*.js", "/*.css", "/*.ico").permitAll()
                            // 允许 WebSocket 连接
                            .requestMatchers("/chat/**", "/ws/**").permitAll()
                            // 允许登录注册接口
                            .requestMatchers("/api/v1/users/register", "/api/v1/users/login").permitAll()
                            // 允许测试接口
                            .requestMatchers("/api/v1/test/**").permitAll()
                            // 文件上传和下载相关接口 - 普通用户和管理员都可访问
                            .requestMatchers("/api/v1/upload/**", "/api/v1/parse", "/api/v1/documents/download", "/api/v1/documents/preview").hasAnyRole("USER", "ADMIN")
                            // 对话历史相关接口 - 用户只能查看自己的历史，管理员可以查看所有
                            .requestMatchers("/api/v1/users/conversation/**").hasAnyRole("USER", "ADMIN")
                            // 搜索接口 - 普通用户和管理员都可访问
                            .requestMatchers("/api/search/**").hasAnyRole("USER", "ADMIN")
                            // 聊天相关接口 - WebSocket停止Token获取 (允许匿名访问)
                            .requestMatchers("/api/chat/websocket-token").permitAll()
                            // 管理员专属接口 - 知识库管理、系统状态、用户活动监控
                            .requestMatchers("/api/v1/admin/**").hasRole("ADMIN")
                            // 用户组织标签管理接口
                            .requestMatchers("/api/v1/users/primary-org").hasAnyRole("USER", "ADMIN")
                            // 其他请求需要认证
                            .anyRequest().authenticated())
                    // 关闭session，只使用JWT进行认证
                    .sessionManagement(session -> session
                            .sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                    // 添加JWT认证过滤器
                    // UsernamePasswordAuthenticationFilter是Spring Security默认的认证过滤器
                    // 它专门处理 POST 请求且路径为 /login 的表单提交。它会从请求体中提取 username 和 password 参数
                    // 我们将自定义的 JwtAuthenticationFilter 放在它之前，以确保 JWT 认证优先处理
                    .addFilterBefore(jwtAuthenticationFilter, UsernamePasswordAuthenticationFilter.class)
                    // 添加组织标签授权过滤器
                    .addFilterAfter(orgTagAuthorizationFilter, JwtAuthenticationFilter.class);

            // 记录安全配置加载成功的信息
            logger.info("Security configuration loaded successfully.");
            // 返回配置好的安全过滤链
            return http.build();
        } catch (Exception e) {
            // 记录配置安全过滤链失败的错误信息
            logger.error("Failed to configure security filter chain", e);
            // 抛出异常，以便外部处理
            throw e;
        }
    }

    // 2. 专业的跨域配置源
    @Bean
    public CorsConfigurationSource corsConfigurationSource() {
        CorsConfiguration configuration = new CorsConfiguration();
        configuration.setAllowedOriginPatterns(Arrays.asList("*")); // 允许所有来源，生产环境建议指定具体域名
        configuration.setAllowedMethods(Arrays.asList("GET", "POST", "PUT", "DELETE", "OPTIONS"));
        configuration.setAllowedHeaders(Arrays.asList("*"));
        configuration.setAllowCredentials(true); // 允许携带 Cookie/Auth Header

        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", configuration); // 对所有路径生效
        return source;
    }
}

