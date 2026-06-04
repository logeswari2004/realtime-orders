package com.orders.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * JPA entity mapped to the 'orders' table.
 * Used for the REST endpoint that returns all current orders on page load.
 */
@Entity
@Table(name = "orders")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    @Column(name = "customer_name", nullable = false, length = 100)
    private String customerName;

    @Column(name = "product_name", nullable = false, length = 100)
    private String productName;

    @Column(nullable = false, length = 20)
    private String status;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    @PrePersist
    @PreUpdate
    private void touchTimestamp() {
        this.updatedAt = LocalDateTime.now();
    }
}
