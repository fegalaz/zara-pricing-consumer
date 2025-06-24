package com.test_zara.zara_pricing_consumer.infrastructure.adapters.output;

import com.test_zara.zara_pricing_consumer.domain.PricingEventDocument;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface PricingEventRepository extends MongoRepository<PricingEventDocument, String> {
}
