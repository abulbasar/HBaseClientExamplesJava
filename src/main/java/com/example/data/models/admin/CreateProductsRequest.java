package com.example.data.models.admin;

import com.example.data.models.ProductRequest;

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
