package org.example.aspect;

import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.example.annotation.LogAction;
import org.example.utils.LogUtils;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.web.multipart.MultipartFile;

@Aspect
@Component
@Slf4j
public class LogAspect {

    // 排除掉一些无法被 JSON 序列化的参数，如 MultipartFile, HttpServletResponse
    private static final Class<?>[] IGNORED_CLASSES = {
            ServletRequest.class, ServletResponse.class, MultipartFile.class
    };

    @Around("@annotation(logAction)")
    public Object doAround(ProceedingJoinPoint joinPoint, LogAction logAction) throws Throwable {
        // 1. 获取基础信息
        String module = logAction.value();
        String action = logAction.action();

        // 2. 获取 Request 上下文
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        HttpServletRequest request = attributes != null ? attributes.getRequest() : null;

        String userId = "UNKNOWN";
        String clientIp = "0.0.0.0";
        if (request != null) {
            userId = String.valueOf(request.getAttribute("userId"));
            clientIp = request.getRemoteAddr();
        }

        // 3. 开启性能监控
        LogUtils.PerformanceMonitor monitor = LogUtils.startPerformanceMonitor(module + "-" + action);

        // 4. 自动记录入参 (通用化关键)
        if (logAction.logArgs()) {
            String argsJson = getArgsAsJson(joinPoint);
            LogUtils.logBusiness(module, userId, "[%s] 请求开始, IP: %s, 参数: %s", action, clientIp, argsJson);
        }

        try {
            // 5. 执行业务逻辑
            Object result = joinPoint.proceed();

            // 6. 成功记录
            LogUtils.logUserOperation(userId, module, action, "SUCCESS");
            monitor.end("执行成功");
            return result;

        } catch (Throwable e) {
            // 7. 异常记录 (自动配合GlobalExceptionHandler)
            LogUtils.logBusinessError(module, userId, action + " 执行异常", (Exception) e);
            monitor.end("执行失败: " + e.getMessage());
            throw e;
        }
    }

    /**
     * 通用的参数序列化方法：自动过滤文件流等不可序列化对象
     */
    private String getArgsAsJson(ProceedingJoinPoint joinPoint) {
        Object[] args = joinPoint.getArgs();
        StringBuilder sb = new StringBuilder();
        for (Object arg : args) {
            if (arg != null && !isIgnored(arg)) {
                sb.append(arg.toString()).append(" ");
                // 或者使用 Jackson: objectMapper.writeValueAsString(arg)
            }
        }
        return sb.toString();
    }

    private boolean isIgnored(Object arg) {
        for (Class<?> clazz : IGNORED_CLASSES) {
            if (clazz.isInstance(arg)) return true;
        }
        return false;
    }
}