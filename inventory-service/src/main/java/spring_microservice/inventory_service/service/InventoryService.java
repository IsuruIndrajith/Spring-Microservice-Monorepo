package spring_microservice.inventory_service.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import spring_microservice.inventory_service.repository.InventoryRepository;

@Service
@RequiredArgsConstructor
public class InventoryService {
    private final InventoryRepository inventoryRepository;

    @Transactional(readOnly = true)
    public boolean isInStock(String skuCode){
        return inventoryRepository.findBySkuCode(skuCode)
                .map(inventory -> inventory.getQuantity() > 0)
                .orElse(false);
    }
}

