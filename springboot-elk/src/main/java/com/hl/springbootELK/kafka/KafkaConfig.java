package com.hl.springbootELK.kafka;

import org.springframework.stereotype.Component;

@Component
//@ConfigurationProperties(prefix = "kafkaConfig")
public class KafkaConfig {

    private String moduleName = "user";

    public static final class LogType{
        public static String INFO="info";
        public static String ERROR="error";
    }

    public static final String qgKey="qg";

    public String getModuleName() {
        return moduleName;
    }

    public void setModuleName(String moduleName) {
        this.moduleName = moduleName;
    }
}
