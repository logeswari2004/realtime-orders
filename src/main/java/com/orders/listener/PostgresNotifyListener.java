package com.orders.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orders.model.OrderEvent;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Listens on the PostgreSQL 'order_changes' channel using LISTEN/NOTIFY.
 *
 * Design rationale
 * ----------------
 * PostgreSQL LISTEN/NOTIFY is a lightweight, built-in pub/sub mechanism.
 * A single dedicated JDBC connection issues "LISTEN order_changes" and then
 * polls PGConnection.getNotifications() every 500 ms.  This is NOT the same
 * as HTTP polling — the JDBC driver's getNotifications() call is cheap and
 * returns only what the database has already pushed into the driver's buffer.
 * No database query is executed per poll; the cost is just a buffer check.
 *
 * Whenever a notification arrives the payload (JSON) is deserialized into an
 * OrderEvent and forwarded to all WebSocket subscribers on /topic/orders via
 * Spring's STOMP messaging template.
 *
 * Reconnection
 * ------------
 * If the dedicated connection drops, a background thread attempts to reconnect
 * every 5 seconds so the system recovers from transient DB outages without
 * requiring a service restart.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PostgresNotifyListener {

    private static final String CHANNEL          = "order_changes";
    private static final String STOMP_TOPIC      = "/topic/orders";
    private static final long   POLL_INTERVAL_MS = 500;
    private static final long   RECONNECT_DELAY_S = 5;

    private final DataSource              dataSource;
    private final SimpMessagingTemplate   messagingTemplate;
    private final ObjectMapper            objectMapper;

    /** Dedicated long-lived connection used only for LISTEN. */
    private Connection              listenConnection;
    private PGConnection            pgConnection;

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(
                    r -> new Thread(r, "pg-notify-listener"));

    private ScheduledFuture<?> pollTask;
    private final AtomicBoolean running = new AtomicBoolean(false);

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    @PostConstruct
    public void start() {
        connect();
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        if (pollTask != null) {
            pollTask.cancel(false);
        }
        scheduler.shutdown();
        closeConnection();
        log.info("PostgresNotifyListener stopped.");
    }

    // ------------------------------------------------------------------
    // Connection management
    // ------------------------------------------------------------------

    private void connect() {
        try {
            listenConnection = dataSource.getConnection();
            // Unwrap the PostgreSQL-specific interface to access notifications
            pgConnection = listenConnection.unwrap(PGConnection.class);

            // Register interest in the channel
            try (Statement stmt = listenConnection.createStatement()) {
                stmt.execute("LISTEN " + CHANNEL);
            }

            running.set(true);
            log.info("Connected to PostgreSQL and listening on channel '{}'", CHANNEL);

            // Schedule the notification poll loop
            pollTask = scheduler.scheduleAtFixedRate(
                    this::pollNotifications,
                    0,
                    POLL_INTERVAL_MS,
                    TimeUnit.MILLISECONDS);

        } catch (Exception ex) {
            log.error("Failed to connect for LISTEN, will retry in {}s: {}", RECONNECT_DELAY_S, ex.getMessage());
            scheduleReconnect();
        }
    }

    private void scheduleReconnect() {
        running.set(false);
        closeConnection();
        scheduler.schedule(this::connect, RECONNECT_DELAY_S, TimeUnit.SECONDS);
    }

    private void closeConnection() {
        try {
            if (listenConnection != null && !listenConnection.isClosed()) {
                listenConnection.close();
            }
        } catch (Exception ignored) {}
    }

    // ------------------------------------------------------------------
    // Notification polling and dispatch
    // ------------------------------------------------------------------

    /**
     * Called every POLL_INTERVAL_MS milliseconds.
     * PGConnection.getNotifications() returns any notifications the driver
     * has buffered since the last call — no SQL query is issued.
     */
    private void pollNotifications() {
        if (!running.get()) return;

        try {
            PGNotification[] notifications = pgConnection.getNotifications(0);
            if (notifications == null) return;

            for (PGNotification notification : notifications) {
                handleNotification(notification);
            }
        } catch (Exception ex) {
            log.error("Lost connection to PostgreSQL listener: {}", ex.getMessage());
            scheduleReconnect();
        }
    }

    private void handleNotification(PGNotification notification) {
        String payload = notification.getParameter();
        log.debug("Received notification on '{}': {}", notification.getName(), payload);

        try {
            OrderEvent event = objectMapper.readValue(payload, OrderEvent.class);
            messagingTemplate.convertAndSend(STOMP_TOPIC, event);
            log.info("Broadcasted {} event for order id={}", event.getOperation(), event.getId());
        } catch (Exception ex) {
            log.error("Failed to parse or broadcast notification payload: {}", payload, ex);
        }
    }
}
