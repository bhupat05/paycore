package com.paycore.paymentledger.service;

import com.paycore.paymentledger.api.dto.PaymentDtos.PaymentResponse;
import com.paycore.paymentledger.domain.Payment;
import com.paycore.paymentledger.exception.PaymentNotFoundException;
import com.paycore.paymentledger.repository.PaymentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class PaymentQueryService {

    private final PaymentRepository paymentRepository;

    public PaymentResponse getPayment(UUID paymentId) {
        Payment payment = paymentRepository.findById(paymentId)
            .orElseThrow(() -> new PaymentNotFoundException(paymentId));
        return toResponse(payment);
    }

    /**
     * Most recent payments first, across all accounts — powers the "recent
     * activity" feed in the frontend rather than any single account's view.
     */
    public Page<PaymentResponse> listPayments(Pageable pageable) {
        return paymentRepository.findAllByOrderByCreatedAtDesc(pageable).map(this::toResponse);
    }

    private PaymentResponse toResponse(Payment payment) {
        return new PaymentResponse(payment.getId(), payment.getFromAccountId(), payment.getToAccountId(),
            payment.getAmount(), payment.getCurrency(), payment.getStatus().name(), payment.getFailureReason(),
            payment.getCreatedAt(), payment.getUpdatedAt());
    }
}
