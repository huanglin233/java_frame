package com.hl.cloud.order8000.service.Impl;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.stereotype.Component;

import com.hl.cloud.order8000.service.LoadBalancer;

@Component
public class MyLB implements LoadBalancer{
    private static final Logger logger = LoggerFactory.getLogger(MyLB.class);

    private AtomicInteger atomicInteger = new AtomicInteger(0);

    public final int getAndInstance() {
        int current;
        int next;
        do {
            current = this.atomicInteger.get();
            next    = current >= 2147483637 ? 0 : current + 1;
        } while(this.atomicInteger.compareAndSet(current, next));
        logger.info("-----next: " + next);

        return next;
    }

    @Override
    public ServiceInstance instances(List<ServiceInstance> serverInstances) {
        int index = getAndInstance() % serverInstances.size();

        return serverInstances.get(index);
    }

}
