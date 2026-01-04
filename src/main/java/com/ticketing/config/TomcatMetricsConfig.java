package com.ticketing.config;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import org.apache.catalina.Service;
import org.apache.tomcat.util.threads.ThreadPoolExecutor;
import org.springframework.boot.web.context.WebServerInitializedEvent;
import org.springframework.boot.web.embedded.tomcat.TomcatWebServer;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Configuration;
import org.springframework.lang.NonNull;

import java.util.Optional;

@Configuration
@RequiredArgsConstructor
public class TomcatMetricsConfig implements ApplicationListener<WebServerInitializedEvent> {

    private final MeterRegistry meterRegistry;

    @Override
    public void onApplicationEvent(@NonNull WebServerInitializedEvent event) {
        Optional.ofNullable(event.getWebServer())
                .filter(server -> server instanceof TomcatWebServer)
                .map(server -> (TomcatWebServer) server)
                .map(TomcatWebServer::getTomcat)
                .ifPresent(tomcat -> {
                    for (Service service : tomcat.getServer().findServices()) {
                        for (org.apache.catalina.connector.Connector connector : service.findConnectors()) {
                            if (connector.getProtocolHandler().getExecutor() instanceof ThreadPoolExecutor executor) {
                                registerThreadMetrics(executor);
                            }
                        }
                    }
                });
    }

    private void registerThreadMetrics(ThreadPoolExecutor executor) {
        Gauge.builder("tomcat_threads_active", executor, ThreadPoolExecutor::getActiveCount)
                .description("Number of active threads in Tomcat thread pool")
                .register(meterRegistry);

        Gauge.builder("tomcat_threads_max", executor, ThreadPoolExecutor::getMaximumPoolSize)
                .description("Maximum number of threads in Tomcat thread pool")
                .register(meterRegistry);

        Gauge.builder("tomcat_threads_current", executor, ThreadPoolExecutor::getPoolSize)
                .description("Current number of threads in Tomcat thread pool")
                .register(meterRegistry);

        Gauge.builder("tomcat_threads_queue_size", executor, exec -> exec.getQueue().size())
                .description("Number of queued tasks in Tomcat thread pool")
                .register(meterRegistry);
    }
}

