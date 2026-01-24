package cz.ivosahlik.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@Profile("local")
public class AutoCreateConfig {

    @Value("${spring.kafka.topic}")
    public String topic;

    @Value("${spring.kafka.partitions}")
    public int partitions;

    @Value("${spring.kafka.replicas}")
    public int replicas;

    @Bean
    public NewTopic libraryEvents(){
        return TopicBuilder.name(topic)
                .partitions(partitions)
                .replicas(replicas)
                .build();
    }

}
