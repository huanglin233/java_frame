package com.hl.cloud.gateway.config;

import org.springframework.context.annotation.Configuration;

@Configuration
public class GatewayConfig {

//    @Bean
//    public RouteLocator customeRouteLocator(RouteLocatorBuilder locatorBuilder) {
//        Builder routes = locatorBuilder.routes();
//        // 当访问 /payment/get/**路径时,路由到http://locahost:8002/上
//        routes.route("payment_8002", r -> r.path("/payment/get/**").uri("http://localhost:8002/")).build();
//
//        return routes.build();
//    }
}