package com.example.models;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class ProductRequest {

    private String id;
    private String name;
    private String author;
    private int pageCount;
    private double price;

    public ProductRequest(){}

    public String getExternalId() {
        return id;
    }

    public void setExternalId(String externalId) {
        this.id = externalId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public int getPageCount() {
        return pageCount;
    }

    public void setPageCount(int pageCount) {
        this.pageCount = pageCount;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }


    public Put toPut(){
        Put put = new Put(Bytes.toBytes(id));
        byte[] colFamily = Bytes.toBytes("info");
        put.addColumn(colFamily, Bytes.toBytes("name"), Bytes.toBytes(name));
        put.addColumn(colFamily, Bytes.toBytes("author"), Bytes.toBytes(author));
        put.addColumn(colFamily, Bytes.toBytes("pageCount"), Bytes.toBytes(pageCount));
        put.addColumn(colFamily, Bytes.toBytes("price"), Bytes.toBytes(price));

        return put;
    }
}
