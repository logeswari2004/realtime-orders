package com.orders.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents the JSON payload emitted by the PostgreSQL trigger function.
 * This DTO is deserialized from the pg_notify payload and then forwarded
 * to connected WebSocket clients.
 *
 * Example payload:
 * {
 *   "operation": "UPDATE",
 *   "id": 1,
 *   "customerName": "Alice Johnson",
 *   "productName": "Laptop Pro 15",
 *   "status": "shipped",
 *   "updatedAt": "2024-06-04T10:30:00"
 * }
 */
@Data
@NoArgsConstructor
public class OrderEvent {

    /** The DML operation: INSERT | UPDATE | DELETE */
    private String operation;

    private Integer id;

    @JsonProperty("customerName")
    private String customerName;

    @JsonProperty("productName")
    private String productName;

    private String status;

    private String updatedAt;
}
