package com.orders.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

/**
 * Configures the STOMP WebSocket message broker.
 *
 * Two key pieces:
 *  1. /ws  – the SockJS endpoint clients connect to
 *  2. /topic – prefix for server-to-client broadcast destinations
 *
 * SockJS is enabled as a fallback transport for environments
 * where native WebSocket is unavailable (e.g. some corporate proxies).
 */
@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry.addEndpoint("/ws")
                .setAllowedOriginPatterns("*")   // Restrict to your domain in production
                .withSockJS();
    }

    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        // Clients subscribe to destinations prefixed with /topic
        registry.enableSimpleBroker("/topic");

        // Client-to-server messages would be prefixed with /app (not used here,
        // but kept for future bidirectional features e.g. order creation via WS)
        registry.setApplicationDestinationPrefixes("/app");
    }
}
