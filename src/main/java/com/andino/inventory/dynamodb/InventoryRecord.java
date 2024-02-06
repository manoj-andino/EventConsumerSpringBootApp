package com.andino.inventory.dynamodb;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@DynamoDBTable(tableName = "inventory")
public class InventoryRecord {
    @DynamoDBHashKey(attributeName = "product_id")
    private String productId;

    @DynamoDBAttribute(attributeName = "allocation")
    private Long allocation;

    @DynamoDBAttribute(attributeName = "source_sync_timestamp")
    private Long sourceSyncTimestamp;
}
