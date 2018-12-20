package com.example.models;

import java.util.List;

public class GenericResponse {
    private String status = "Success";
    private String message = null;
    private Integer size = -1;
    private List<String> records = null;

    public GenericResponse(){

    }


    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }


    public List<String> getRecords() {
        return records;
    }

    public void setRecords(List<String> records) {
        this.records = records;
        this.size = records.size();
    }

    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }
}
