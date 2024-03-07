package com.hl.springbootMinio.config;

import com.hl.springbootMinio.utils.MinioUtils;
import io.minio.MinioClient;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;

/**
 * @author huanglin
 * @date 2023/09/03 20:06
 */
@Configuration
@EnableConfigurationProperties(MinioProperties.class)
public class MinioConfig {

    @Resource
    private MinioProperties minioProperties;

    @Bean
    public MinioUtils minioClient() {
        MinioClient build = MinioClient.builder()
                .endpoint(minioProperties.getEndpoint())
                .credentials(minioProperties.getAccessKey(), minioProperties.getSecretKey())
                .build();

        return new MinioUtils(build);
    }
}
