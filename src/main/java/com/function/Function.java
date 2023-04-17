package com.function;

import com.azure.core.credential.AzureKeyCredential;
import com.azure.core.implementation.util.EnvironmentConfiguration;
import com.azure.cosmos.*;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.util.CosmosPagedFlux;
import com.fasterxml.jackson.core.Base64Variants;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * Azure Functions with HTTP Trigger.
 */
public class Function {
        /**
         * This function listens at endpoint "/api/getlastdata".
         */
        @FunctionName("getlastdata")
        public HttpResponseMessage run(
                        @HttpTrigger(name = "req", methods = {
                                        HttpMethod.GET }, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
                        final ExecutionContext context) {

                LocalDate today = LocalDate.now();
                LocalDateTime midnight = LocalDateTime.of(today, LocalTime.MIDNIGHT);
                DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                                .appendPattern("yyyy-MM-dd'T'HH:mm:ss")
                                .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                                .appendOffsetId()
                                .toFormatter();
                String midnightString = formatter.format(midnight.atZone(ZoneOffset.UTC));

                final String endpoint = System.getenv("endpointcosmos");
                final String key = System.getenv("cosmosKey");
                final String databaseId = System.getenv("DBName");
                final String containerId = System.getenv("ContainerName");

                CosmosClientBuilder clientBuilder = new CosmosClientBuilder()
                                .endpoint(endpoint)
                                .credential(new AzureKeyCredential(key))
                                .consistencyLevel(ConsistencyLevel.EVENTUAL);

                CosmosAsyncClient client = clientBuilder.buildAsyncClient();

                CosmosAsyncContainer container = client.getDatabase(databaseId).getContainer(containerId);

                String query = "SELECT {\n" +
                                "    \"Body\": c.Body,\n" +
                                "    \"Time\": c.SystemProperties[\"iothub-enqueuedtime\"]\n" +
                                "}\n" +
                                "FROM c \n" +
                                "WHERE c.SystemProperties[\"iothub-enqueuedtime\"] > '" + midnightString + "'";

                CosmosQueryRequestOptions options = new CosmosQueryRequestOptions();
                options.setMaxDegreeOfParallelism(10);

                CosmosPagedFlux<JsonNode> queryResults = container.queryItems(query, options, JsonNode.class);

                List<JsonNode> items = queryResults.collectList().block();

                for (JsonNode item : items) {
                        ObjectNode objectNode = (ObjectNode) item;
                        String base64EncodedBody = objectNode.get("$1").get("Body").asText();
                        byte[] decodedBody = Base64Variants.getDefaultVariant().decode(base64EncodedBody);
                        String decodedBodyString = new String(decodedBody);
                        ObjectNode temp = (ObjectNode) objectNode.get("$1");
                        temp.put("Body", decodedBodyString);
                }

                client.close();

                if (items == null) {
                        return request.createResponseBuilder(HttpStatus.NOT_FOUND)
                                        .body("No items found in Cosmos DB container")
                                        .build();
                } else {
                        return request.createResponseBuilder(HttpStatus.OK)
                                        .header("Content-Type", "application/json")
                                        .body(items)
                                        .build();
                }
        }
}
