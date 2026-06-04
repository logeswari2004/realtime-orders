package com.orders.controller;

import com.orders.model.Order;
import com.orders.service.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * REST API for the orders resource.
 *
 * Endpoints
 * ---------
 *  GET    /api/orders           – fetch all orders (used on initial page load)
 *  POST   /api/orders           – create a new order
 *  PATCH  /api/orders/{id}      – update an order's status
 *  DELETE /api/orders/{id}      – delete an order
 *
 * Every write operation triggers the PostgreSQL trigger, which fires
 * pg_notify → PostgresNotifyListener picks it up → broadcasts over WebSocket.
 * The REST layer itself does NOT push to WebSocket; it only writes to the DB.
 */
@Slf4j
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
@CrossOrigin(origins = "*")  // Restrict to your domain in production
public class OrderController {

    private final OrderRepository orderRepository;

    /** Return all orders sorted by id for the initial page load. */
    @GetMapping
    public List<Order> getAllOrders() {
        return orderRepository.findAll();
    }

    /** Create a new order. Triggers INSERT → pg_notify. */
    @PostMapping
    public ResponseEntity<Order> createOrder(@RequestBody Order order) {
        Order saved = orderRepository.save(order);
        log.info("Created order id={}", saved.getId());
        return ResponseEntity.ok(saved);
    }

    /** Update the status of an existing order. Triggers UPDATE → pg_notify. */
    @PatchMapping("/{id}")
    public ResponseEntity<Order> updateStatus(
            @PathVariable Integer id,
            @RequestBody Map<String, String> body) {

        return orderRepository.findById(id)
                .map(order -> {
                    String newStatus = body.get("status");
                    if (newStatus == null || newStatus.isBlank()) {
                        return ResponseEntity.badRequest().<Order>build();
                    }
                    // Status flow validation: pending → shipped → delivered only
                    if (!isValidTransition(order.getStatus(), newStatus)) {
                        log.warn("Rejected invalid transition: {} → {} for order id={}",
                                order.getStatus(), newStatus, id);
                        return ResponseEntity.badRequest().<Order>build();
                    }
                    order.setStatus(newStatus);
                    Order updated = orderRepository.save(order);
                    log.info("Updated order id={} → status={}", id, newStatus);
                    return ResponseEntity.ok(updated);
                })
                .orElse(ResponseEntity.notFound().build());
    }

    /** Delete an order. Triggers DELETE → pg_notify. */
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteOrder(@PathVariable Integer id) {
        if (!orderRepository.existsById(id)) {
            return ResponseEntity.notFound().build();
        }
        orderRepository.deleteById(id);
        log.info("Deleted order id={}", id);
        return ResponseEntity.noContent().build();
    }

    /**
     * Enforces one-directional status flow:
     *   pending → shipped → delivered
     *
     * Any backward or same-state transition is rejected.
     * This check runs at the API layer so even direct API calls
     * (e.g. curl, Postman) cannot violate business rules.
     */
    private boolean isValidTransition(String current, String next) {
        return switch (current) {
            case "pending"   -> "shipped".equals(next);
            case "shipped"   -> "delivered".equals(next);
            case "delivered" -> false; // final state — no further transitions
            default          -> false;
        };
    }
}
