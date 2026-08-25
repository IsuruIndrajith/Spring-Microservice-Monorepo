package com.microservice.product.service.repository;

import com.microservice.product.service.model.Product;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface ProductRepository extends MongoRepository<Product, String>{
}
