package com.microsoft.learn.azure.cosmosdb.table.quickstart;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessageSendingOperations;
import org.springframework.stereotype.Controller;

import com.azure.data.tables.models.TableEntity;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import reactor.core.publisher.Mono;

@Controller
public class StartController {

    private static SimpMessageSendingOperations messageSendingOperations;

    private EntityRepository repository;

    @Autowired
    public StartController(SimpMessageSendingOperations messageSendingOperations, EntityRepository repository) {
        StartController.messageSendingOperations = messageSendingOperations;
        this.repository = repository;
    }

    @MessageMapping("/start")
    public Mono<Void> startAsync() throws Exception {
        this.sendMessage("Current Status:\t\tStarting...");

        this.sendMessage(String.format("Get table:\t\t%s", "cosmicworks-products"));

        TableEntity firstEntity = new TableEntity("gear-surf-surfboards", "aaaaaaaa-0000-1111-2222-bbbbbbbbbbbb")
                .addProperty("name", "Yamba Surfboard")
                .addProperty("quantity", 12)
                .addProperty("onSale", false);

        Mono<Void> firstUpsertMono = repository.upsertEntity(firstEntity)
                .doOnSuccess(unused -> {
                    this.sendMessage(String.format("Upserted entity:\t%s", getJsonString(firstEntity)));
                })
                .then();

        TableEntity secondEntity = new TableEntity("gear-surf-surfboards", "bbbbbbbb-1111-2222-3333-cccccccccccc")
                .addProperty("name", "Kiama Classic Surfboard")
                .addProperty("quantity", 4)
                .addProperty("onSale", true);

        Mono<Void> secondUpsertMono = repository.upsertEntity(secondEntity)
                .doOnSuccess(nullValue -> {
                    this.sendMessage(String.format("Upserted entity:\t%s", getJsonString(secondEntity)));
                })
                .then();
                
        Mono<Void> readMono = repository.getEntityByRowKeyAndPartitionKey("aaaaaaaa-0000-1111-2222-bbbbbbbbbbbb", "gear-surf-surfboards")
            .doOnSuccess(exiting_entity -> {
                this.sendMessage(String.format("Read entity:\t\t%s", getJsonString(exiting_entity)));
            })
            .then();

        Mono<Void> queryMono = repository.getEntitiesByPartitionKey("gear-surf-surfboards")
                .doFirst(() -> {
                    this.sendMessage("Current Status:\t\tQuerying entities...");
                })
                .doOnNext(entity -> {
                    this.sendMessage(String.format("Found entity:\t\t%s", getJsonString(entity)));
                })
                .doOnTerminate(() -> {                    
                    this.sendMessage("Current Status:\t\tStopping...");
                })
                .then();

        return Mono.empty()
            .then(firstUpsertMono)
            .then(secondUpsertMono)
            .then(readMono)
            .then(queryMono)
            .then();
    }

    private void sendMessage(String message) {
        StartController.messageSendingOperations.convertAndSend("/topic/output", new Payload(message));
    }

    private String getJsonString(TableEntity entity) {
        return String.format("[%s]\t%s", entity.getRowKey(), entity.getProperty("name"));
    }
}
