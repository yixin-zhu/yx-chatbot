package org.example.annotation;

import java.lang.annotation.*;

@Target(ElementType.METHOD) // 作用在方法上
@Retention(RetentionPolicy.RUNTIME) // 运行时有效
@Documented
public @interface LogAction {
    String value() default "";    // 操作名称，如 "搜索文档"
    String action() default "";   // 动作类型，如 "SEARCH"
    boolean logArgs() default true; // 是否自动记录入参
}