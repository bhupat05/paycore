package com.paycore.paymentledger.repository;

import com.paycore.paymentledger.domain.Notification;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface NotificationRepository extends JpaRepository<Notification, UUID> {
}
