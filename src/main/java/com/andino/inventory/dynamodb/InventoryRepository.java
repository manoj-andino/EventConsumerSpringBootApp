package com.andino.inventory.dynamodb;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Repository;

@Repository
@AllArgsConstructor
public class InventoryRepository {
    final private DynamoDBMapper dynamoDBMapper;

    public String createInventoryRecord(InventoryRecord inventoryRecord) {
        dynamoDBMapper.save(inventoryRecord);
        return inventoryRecord.getProductId();
    }

    public InventoryRecord getInventoryByProductId(String productId) {
        return dynamoDBMapper.load(InventoryRecord.class, productId);
    }

    public InventoryRecord updateInventoryByProductId(String productId, Long allocation) {
        InventoryRecord load = dynamoDBMapper.load(InventoryRecord.class, productId);
        load.setAllocation(allocation);
        dynamoDBMapper.save(load);

        return dynamoDBMapper.load(InventoryRecord.class, productId);
    }

    public String deleteInventoryByProductId(String productId) {
        InventoryRecord load = dynamoDBMapper.load(InventoryRecord.class, productId);
        dynamoDBMapper.delete(load);
        return load.getProductId();
    }
}
