package main.java.ms_campuslab_kafka.shared;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean(detroyMethod = "close")
    public AdminClient adminClient(){

        Map<String, Object> properties = new HashMap<>();

        properties.put(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, 
            bootstrapServers
        );
        
        return AdminClient.create(properties);
    }

}
