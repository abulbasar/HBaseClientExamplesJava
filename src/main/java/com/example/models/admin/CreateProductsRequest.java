package com.example.models.admin;

import com.example.models.ProductRequest;

import java.util.List;

public class CreateProductsRequest {

    private List<ProductRequest> requests;

    public List<ProductRequest> getRequests() {
        return requests;
    }

    public void setRequests(List<ProductRequest> requests) {
        this.requests = requests;
    }

    public CreateProductsRequest(){}
}
