package com.microsoft.learn.azure.cosmosdb.table.quickstart;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.azure.core.http.rest.PagedFlux;
import com.azure.data.tables.TableAsyncClient;
import com.azure.data.tables.models.ListEntitiesOptions;
import com.azure.data.tables.models.TableEntity;
import reactor.core.publisher.Mono;

@Service
public class EntityRepository {
    @Autowired
    private TableAsyncClient tableAsyncClient;

    public Mono<TableEntity> getEntityByRowKeyAndPartitionKey(String rowKey, String partitionKey) {
        return tableAsyncClient.getEntity(partitionKey, rowKey);
    }

    public Mono<Void> upsertEntity(TableEntity entity) {
        return tableAsyncClient.upsertEntity(entity);
    }

    public PagedFlux<TableEntity> getEntitiesByPartitionKey(String partitionKey) {
        ListEntitiesOptions options = new ListEntitiesOptions()
                .setFilter(String.format("PartitionKey eq '%s'", partitionKey));

        return tableAsyncClient.listEntities(options);
    }
}