package com.test_zara.zara_pricing_consumer.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "pricing_events")
public class PricingEventDocument {
    
    @Id
    private String id;
    
    private String productId;
    private String productName;
    private Double oldPrice;
    private Double newPrice;
    private String currency;
    private String changeType;
    private Long timestamp;
    private String storeId;
    private String category;
} 