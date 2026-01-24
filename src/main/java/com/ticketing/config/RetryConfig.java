package com.ticketing.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.annotation.EnableRetry;

@Configuration
@EnableRetry
@Slf4j
public class RetryConfig {

    @Bean
    public RetryListener retryListener() {
        return new RetryListener() {
            
            @Override
            public <T, E extends Throwable> void onError(
                    RetryContext context, 
                    RetryCallback<T, E> callback, 
                    Throwable throwable
            ) {
                int retryCount = context.getRetryCount();
                String errorMessage = throwable.getMessage();
                
                if (retryCount > 0) {
                    log.warn("재시도 {}차 실패: error={}", retryCount, errorMessage);
                } else {
                    log.warn("처리 실패 - 재시도 예정: error={}", errorMessage);
                }
            }
            
            @Override
            public <T, E extends Throwable> void onSuccess(
                    RetryContext context, 
                    RetryCallback<T, E> callback, 
                    T result
            ) {
                int retryCount = context.getRetryCount();
                if (retryCount > 0) {
                    log.info("재시도 성공: 총 {}차 시도 후 성공", retryCount + 1);
                }
            }
        };
    }
}
