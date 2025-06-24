package com.test_zara.zara_pricing_consumer.infrastructure.adapters.input;

import com.test_zara.zara_pricing_consumer.domain.PricingEventDocument;
import com.test_zara.zara_pricing_consumer.infrastructure.adapters.output.PricingEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class PricingEventKafkaConsumer {

    private final PricingEventRepository repository;

    @KafkaListener(
            topics = "${kafka.topics.pricing-events}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consume(GenericRecord event) {
        log.info("Mensaje recibido: {}", event);
        
        try {
            // Convertir GenericRecord a PricingEventDocument
            PricingEventDocument pricingEvent = new PricingEventDocument();
            
            // Mapeo de campos según la estructura del productor
            pricingEvent.setProductId(getStringValue(event, "productId"));
            pricingEvent.setProductName(getStringValue(event, "productName"));
            pricingEvent.setOldPrice(getDoubleValue(event, "oldPrice"));
            pricingEvent.setNewPrice(getDoubleValue(event, "newPrice"));
            pricingEvent.setCurrency(getStringValue(event, "currency"));
            pricingEvent.setChangeType(getStringValue(event, "changeType"));
            pricingEvent.setTimestamp(getLongValue(event, "timestamp"));
            pricingEvent.setStoreId(getStringValue(event, "storeId"));
            pricingEvent.setCategory(getStringValue(event, "category"));
            
            // Guardar en MongoDB
            repository.save(pricingEvent);
            log.info("Evento guardado en MongoDB: {}", pricingEvent);
            
        } catch (Exception e) {
            log.error("Error al procesar el evento: {}", e.getMessage(), e);
        }
    }
    
    private String getStringValue(GenericRecord record, String fieldName) {
        Object value = record.get(fieldName);
        return value != null ? value.toString() : null;
    }
    
    private Double getDoubleValue(GenericRecord record, String fieldName) {
        Object value = record.get(fieldName);
        if (value == null) return null;
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return Double.valueOf(value.toString());
    }
    
    private Long getLongValue(GenericRecord record, String fieldName) {
        Object value = record.get(fieldName);
        if (value == null) return null;
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return Long.valueOf(value.toString());
    }
}