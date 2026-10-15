package com.paycore.paymentledger.repository;

import com.paycore.paymentledger.domain.ProcessedPaymentEvent;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface ProcessedPaymentEventRepository extends JpaRepository<ProcessedPaymentEvent, UUID> {
}
