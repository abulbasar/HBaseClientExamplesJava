package com.example.data.models.admin;

public class CreateTableRequest {

    private String tableName;
    private int versions;
    private String[] columnFamilies;

    public CreateTableRequest(){}

    public String[] getColumnFamilies() {
        return columnFamilies;
    }

    public void setColumnFamilies(String[] columnFamilies) {
        this.columnFamilies = columnFamilies;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getVersions() {
        return versions;
    }

    public void setVersions(int versions) {
        this.versions = versions;
    }
}
