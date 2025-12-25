package org.example.filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.example.service.CustomUserDetailsService;
import org.example.utils.JwtUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.web.authentication.WebAuthenticationDetailsSource;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;


// 自定义的过滤器，用于解析请求头中的 JWT Token，并验证用户身份。
// 如果 Token 有效，则将用户信息和权限设置到 Spring Security 的context中，后续的请求可以基于用户角色进行授权。
@Component
// OncePerRequestFilter 能保证在一次请求中，过滤器只会被执行一次（防止异步请求或转发时重复执行）
public class JwtAuthenticationFilter extends OncePerRequestFilter {
    // 用于生成和解析 JWT Token的工具类
    @Autowired
    private JwtUtils jwtUtils;

    // 用于加载用户详细信息
    @Autowired
    private CustomUserDetailsService userDetailsService;

    // 需要重写该方法，来实现自定义的过滤逻辑
    // 1. 【提取】从请求中获取关键信息（如 Token 或 Header）
    // 2. 【校验】验证合法性（如 查数据库、查 Redis）
    // 3. 【存入上下文】如果合法，告诉 Spring Security 用户是谁
    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        try {
            // 从请求头中提取 JWT Token
            String token = extractToken(request);
            if (token != null) {
                String newToken = null;
                String username = null;

                // 检查token是否有效
                if (jwtUtils.validateToken(token)) {
                    // 如果Token有效，检查是否需要预刷新
                    if (jwtUtils.shouldRefreshToken(token)) {
                        newToken = jwtUtils.refreshToken(token);
                        if (newToken != null) {
                            logger.info("Token auto-refreshed proactively");
                        }
                    }
                    username = jwtUtils.extractUsernameFromToken(token);
                } else {
                    // 若Token无效/过期，检查是否在宽限期内可以刷新
                    if (jwtUtils.canRefreshExpiredToken(token)) {
                        newToken = jwtUtils.refreshToken(token);
                        if (newToken != null) {
                            logger.info("Expired token refreshed within grace period");
                            username = jwtUtils.extractUsernameFromToken(newToken);
                        }
                    }
                }

                // 如果产生了新token，通过响应头返回给前端
                if (newToken != null) {
                    response.setHeader("New-Token", newToken);
                }

                // 身份与权限装载（Authentication Loading）
                // 最重要的一步。如果认证合法，必须构造一个 UsernamePasswordAuthenticationToken 并塞进Spring Security的上下文中
                if (username != null && !username.isEmpty()) {
                    // 从数据库加载用户详细信息，并打包成 UserDetail 对象
                    // 改良：可以考虑使用缓存（如 Redis）来存储 UserDetails，减少数据库访问
                    UserDetails userDetails = userDetailsService.loadUserByUsername(username);
                    // 构造UsernamePasswordAuthenticationToken，里面包含了UserDetail对象 和 权限
                    UsernamePasswordAuthenticationToken authentication = new UsernamePasswordAuthenticationToken(
                            userDetails, null, userDetails.getAuthorities());
                    // 将当前的请求信息（如 IP 地址、Session ID）存入这个Token中
                    authentication.setDetails(new WebAuthenticationDetailsSource().buildDetails(request));
                    // 将认证信息设置到 Spring Security 的上下文中，后续的请求就能通过 SecurityContextHolder 获取到用户信息
                    SecurityContextHolder.getContext().setAuthentication(authentication);
                }
            }
            // 继续执行过滤链
            filterChain.doFilter(request, response);
        } catch (Exception e) {
            logger.error("Cannot set user authentication: {}", e);
        }
    }

    // 从请求头中提取 JWT Token。
    private String extractToken(HttpServletRequest request) {
        String bearerToken = request.getHeader("Authorization");
        if (bearerToken != null && bearerToken.startsWith("Bearer ")) {
            return bearerToken.substring(7); // 去掉 "Bearer " 前缀
        }
        return null;
    }
}
