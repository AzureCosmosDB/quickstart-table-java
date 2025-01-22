package com.microsoft.learn.azure.cosmosdb.table.quickstart;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.azure.data.tables.TableAsyncClient;
import com.azure.data.tables.TableClientBuilder;
import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;

@Configuration
public class CosmosConfiguration {

    @Value("${CONFIGURATION__AZURECOSMOSDB__ENDPOINT}")
    private String endpoint;

    @Value("${CONFIGURATION__AZURECOSMOSDB__TABLENAME}")
    private String tableName;

    @Bean
    public TableAsyncClient tableClientConfiguration() {
        // <create_client>
        DefaultAzureCredential azureTokenCredential = new DefaultAzureCredentialBuilder()
                .build();

        return new TableClientBuilder()
                .endpoint(endpoint)
                .credential(azureTokenCredential)
                .tableName(tableName)
                .buildAsyncClient();
        // </create_client>
    }
}