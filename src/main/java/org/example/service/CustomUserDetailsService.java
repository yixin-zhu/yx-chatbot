package org.example.service;
import org.example.entity.User;
import org.example.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

// 实现 Spring Security 的 UserDetailsService 接口，用于加载用户的详细信息（包括用户名、密码和权限）。
// 通过用户名从数据库中查找用户，并将其转换为 Spring Security 所需的 UserDetails 格式
@Service
public class CustomUserDetailsService implements UserDetailsService {

    @Autowired
    private UserRepository userRepository; // 用于访问用户数据

    // 通过用户名查数据库，并打包成 UserDetails 对象
    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        // 从数据库中查找用户
        User user = userRepository.findByUsername(username)
                .orElseThrow(() -> new UsernameNotFoundException("User not found"));

        List<SimpleGrantedAuthority> authorities =
                Collections.singletonList(new SimpleGrantedAuthority("ROLE_" + user.getRole().name()));

        // 返回 Spring Security 所需的 UserDetails 对象
        // UserDetails 是一个接口，Spring Security 提供了一个默认实现类 org.springframework.security.core.userdetails.User，我们通常直接使用它
        return new org.springframework.security.core.userdetails.User(
                user.getUsername(),
                user.getPassword(),
                authorities
        );
    }

}