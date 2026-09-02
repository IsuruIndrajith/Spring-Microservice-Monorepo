package com.example.order.service.service;

import com.example.order.service.dto.InventoryResponse;
import com.example.order.service.dto.OrderLineItemsDto;
import com.example.order.service.dto.OrderRequest;
import com.example.order.service.event.OrderPlacedEvent;
import com.example.order.service.model.Order;
import com.example.order.service.model.OrderLineItems;
import com.example.order.service.repository.OrderRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Transactional
public class OrderService {

    private final OrderRepository orderRepository;
    private final WebClient.Builder webClientBuilder;
    private final KafkaTemplate<String, OrderPlacedEvent> kafkaTemplate;
    @Value("${inventory.service.url}")
    private String inventoryServiceUrl;

    public void placeOrder(OrderRequest orderRequest){
        Order order = new Order();
        order.setOrderNumber(UUID.randomUUID().toString());

        List<OrderLineItems> orderLineItems = orderRequest.getOrderLineItemsDtoList()
                .stream()
                .map(this::mapToDto)
                .toList();

        order.setOrderLineItemList(orderLineItems);

        List<String> skuCodes = order.getOrderLineItemList().stream()
                .map(OrderLineItems::getSkuCode)
                .toList();

//        call Inventory service and plcae the order if product is in stock

        InventoryResponse[] inventoryResponsesArray = webClientBuilder.build().get()
                .uri(inventoryServiceUrl + "/api/inventory",
                        uriBuilder -> uriBuilder.queryParam("skuCode", skuCodes).build())
                .retrieve()
                .bodyToMono(InventoryResponse[].class)
                .block();
//        for synchronous requests

        Map<String, Boolean> inventoryBySkuCode = Arrays.stream(inventoryResponsesArray)
                .collect(Collectors.toMap(InventoryResponse::getSkuCode, InventoryResponse::isInStock));
        boolean allProductsInStock = skuCodes.stream()
                .allMatch(skuCode -> Boolean.TRUE.equals(inventoryBySkuCode.get(skuCode)));

        if(allProductsInStock){
            orderRepository.save(order);
            kafkaTemplate.send("notificationTopic", new OrderPlacedEvent(order.getOrderNumber()));
        } else {
            throw new IllegalArgumentException("Product is not in stock, please try again later");
        }
    }

    private OrderLineItems mapToDto(OrderLineItemsDto orderLineItemsDto){
        OrderLineItems orderLineItems = new OrderLineItems();
        orderLineItems.setPrice(orderLineItemsDto.getPrice());
        orderLineItems.setQuantity(orderLineItemsDto.getQuantity());
        orderLineItems.setSkuCode(orderLineItemsDto.getSkuCode());
        return orderLineItems;


    }
}
