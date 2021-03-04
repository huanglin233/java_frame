package com.hl.cloud.gateway.config;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;

import reactor.core.publisher.Mono;

@Component
public class MyLogGatewayFilter implements GlobalFilter, Ordered{
    private static final Logger logger = LoggerFactory.getLogger(MyLogGatewayFilter.class);

    @Override
    public int getOrder() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        logger.info("-----come in MyLogGatewayFilter: " + new Date());
        String username = exchange.getRequest().getQueryParams().getFirst("username");
        if(username == null) {
            logger.info("-----username is null");
            exchange.getResponse().setStatusCode(HttpStatus.NOT_ACCEPTABLE);

            return exchange.getResponse().setComplete();
        }

        return chain.filter(exchange);
    }

}