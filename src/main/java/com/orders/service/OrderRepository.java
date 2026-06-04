package com.orders.service;

import com.orders.model.Order;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * Standard Spring Data JPA repository.
 * Provides findAll(), save(), deleteById(), etc. out of the box.
 */
@Repository
public interface OrderRepository extends JpaRepository<Order, Integer> {
}
