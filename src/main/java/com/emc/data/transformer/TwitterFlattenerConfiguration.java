package com.emc.data.transformer;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.messaging.MessageChannel;

import com.fasterxml.jackson.databind.ObjectMapper;

@Configuration
@EnableIntegration
public class TwitterFlattenerConfiguration {

	@Bean
	TwitterFlattener transformer() {
		return new TwitterFlattener();
	}
	
	@Bean
	ObjectMapper mapper() {
		return new ObjectMapper();
	}
	
	@Bean
	MessageChannel input() {
		return new DirectChannel();
	}
	
	@Bean
	MessageChannel output() {
		return new DirectChannel();
	}
}
