package com.andino.inventory.kafkaconsumer;

import com.andino.inventory.dynamodb.InventoryRepository;
import com.andino.inventory.dynamodb.InventoryRecord;
import com.andino.inventory.xml.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

@Component
@AllArgsConstructor
public class InventoryUpdateConsumer {

    private final InventoryRepository inventoryRepository;

    @KafkaListener(
            containerFactory = "concurrentKafkaListenerContainerFactory",
            topics = "inventory-update",
            groupId = "inventory-update-group"
    )
    public void consume(String payload) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        Record record = objectMapper.readValue(payload, Record.class);
        Optional.ofNullable(record).ifPresent(updateInventoryRecord());
    }

    private Consumer<Record> updateInventoryRecord() {
        return record -> {
            String productId = record.productId().toString();
            Long allocation = Long.valueOf(record.allocation());
            Long allocationTimestampAtUtcEpoch = ZonedDateTime.parse(
                    record.allocationTimestamp(),
                    DateTimeFormatter.ISO_DATE_TIME
                ).toEpochSecond();

            InventoryRecord inventoryByProductId = inventoryRepository.getInventoryByProductId(productId);
            if (Objects.isNull(inventoryByProductId)) {
                InventoryRecord newInventoryRecord = InventoryRecord.builder()
                        .productId(productId)
                        .allocation(allocation)
                        .sourceSyncTimestamp(allocationTimestampAtUtcEpoch)
                        .build();
                inventoryRepository.createInventoryRecord(newInventoryRecord);
            } else if (isEligibleForUpdatingInventoryRecord(inventoryByProductId, allocationTimestampAtUtcEpoch)) {
                inventoryRepository.updateInventoryByProductId(
                        productId,
                        allocation,
                        allocationTimestampAtUtcEpoch);
            }
        };
    }

    private boolean isEligibleForUpdatingInventoryRecord(InventoryRecord inventoryByProductId, Long allocationTimestampAtUtcEpoch) {
        return inventoryByProductId.getSourceSyncTimestamp() < allocationTimestampAtUtcEpoch;
    }
}
