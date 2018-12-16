package com.example.data;

import com.example.data.hbase.HBaseHelper;
import com.example.data.models.GenericResponse;
import com.example.data.models.admin.CreateNamespaceRequest;
import com.example.data.models.admin.CreateTableRequest;
import com.example.data.models.admin.DescribeTableRequest;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@RestController
public class AdminController {

    private static final Logger log = LoggerFactory.getLogger(AdminController.class.getName());


    private static HBaseHelper helper;

    public AdminController(){
        helper = HBaseHelper.getInstance();
    }


    @GetMapping("/namespaces")
    public GenericResponse getNamespaces() {
        GenericResponse response = new GenericResponse();
        try{
            response.setRecords(helper.listNamespaces());
        }catch (IOException ex){
            response.setStatus("Failed");
            response.setMessage(ex.getMessage());
            log.error(ex.getMessage(), ex);
        }
        return response;
    }


    @PostMapping("/create_namespace")
    public GenericResponse putNamespaces(@RequestBody CreateNamespaceRequest request) {
        log.info(String.format("Namespace: %s", request.getNamespace()));

        GenericResponse response = new GenericResponse();
        try{
            helper.createNamespace(request.getNamespace());
        }catch (IOException ex){
            response.setStatus("Failed");
            response.setMessage(ex.getMessage());
            log.error(ex.getMessage(), ex);
        }
        return response;
    }

    @GetMapping("/tables")
    public GenericResponse tables(@RequestParam(value="ns", defaultValue = "<all>") String namespace) {
        log.info(String.format("Namespace: %s", namespace));

        GenericResponse response = new GenericResponse();
        try{
            response.setRecords(helper.listTables(namespace));
        }catch (IOException ex){
            response.setStatus("Failed");
            response.setMessage(ex.getMessage());
            log.error(ex.getMessage(), ex);
        }
        return response;
    }

    @PostMapping("/create_table")
    public GenericResponse putTable(@RequestBody CreateTableRequest request) {
        log.info(String.format("Table: %s", request.getTableName()));
        for(String columnFamily: request.getColumnFamilies()){
            log.info(columnFamily);
        }
        GenericResponse response = new GenericResponse();
        try {
            helper.createTable(request.getTableName()
                    , request.getVersions(), request.getColumnFamilies());
        }catch (IOException ex){
            ex.printStackTrace();
            log.error(ex.getMessage(), ex);
            response.setStatus("Failed");
            response.setMessage(ex.getMessage());
        }
        return response;
    }

    @GetMapping("/describe_table")
    public GenericResponse describeTable(@RequestParam(value="table") String tableName) {
        log.info(String.format("Table: %s", tableName));

        GenericResponse response = new GenericResponse();
        try {
            HTableDescriptor descriptor = helper
                            .getTable(tableName)
                            .getTableDescriptor();

            Stream<String> families = descriptor
                    .getFamilies()
                    .stream()
                    .map(f -> f.getNameAsString());
            Map<String, String> properties = new HashMap<>();
            properties.put("columnFamilies", families.collect(Collectors.joining(",")));

            properties.putAll(descriptor.getConfiguration());

            response.setRecords(properties
                    .entrySet()
                    .stream()
                    .map(p -> String.format("%s: %s",p.getKey(), p.getValue()))
                    .collect(Collectors.toList()));

        }catch (IOException ex){
            ex.printStackTrace();
            log.error(ex.getMessage(), ex);
            response.setStatus("Failed");
            response.setMessage(ex.getMessage());
        }
        return response;
    }

}