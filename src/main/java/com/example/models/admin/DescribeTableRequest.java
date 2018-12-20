package com.example.models.admin;

public class DescribeTableRequest {

    private String tableName;

    public DescribeTableRequest(){}

    private int versions;
    private String[] columnFamilies;

    public String getTableName() {
        return tableName;
    }
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }


}
