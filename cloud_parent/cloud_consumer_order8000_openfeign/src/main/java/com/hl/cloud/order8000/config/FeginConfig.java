package com.hl.cloud.order8000.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import feign.Logger;

@Configuration
public class FeginConfig {

    @Bean
    Logger.Level feginLoggerLevel(){
        return Logger.Level.FULL;
    }
}