package org.example.utils;
import lombok.Data;

@Data
public class Result<T> {
    private Integer code;
    private String message;
    private T data;
    private Boolean success; // 额外增加一个布尔值，方便前端 JS 判断

    // 私有化构造器，强制使用静态工厂方法
    private Result(Integer code, String message, T data, Boolean success) {
        this.code = code;
        this.message = message;
        this.data = data;
        this.success = success;
    }

    // 成功静态方法
    public static <T> Result<T> success(T data) {
        return new Result<>(200, "操作成功", data, true);
    }

    public static <T> Result<T> success(String message, T data) {
        return new Result<>(200, message, data, true);
    }

    // 失败静态方法
    public static <T> Result<T> error(Integer code, String message) {
        return new Result<>(code, message, null, false);
    }
}