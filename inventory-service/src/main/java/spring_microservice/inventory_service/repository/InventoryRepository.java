package spring_microservice.inventory_service.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import spring_microservice.inventory_service.model.Inventory;

import java.util.List;
import java.util.Optional;

public interface InventoryRepository extends JpaRepository<Inventory, Long> {

    List<Inventory> findBySkuCodeIn(List<String> skuCode);


}
