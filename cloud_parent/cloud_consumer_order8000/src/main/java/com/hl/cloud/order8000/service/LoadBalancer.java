package com.hl.cloud.order8000.service;

import java.util.List;

import org.springframework.cloud.client.ServiceInstance;


public interface LoadBalancer {
    ServiceInstance instances(List<ServiceInstance> serverInstances);
}