package com.example;

import com.example.hbase.HBaseHelper;
import com.example.models.GenericResponse;
import com.example.models.admin.CreateNamespaceRequest;
import com.example.models.admin.CreateTableRequest;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
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

            List<Pair<byte[], byte[]>> regions = helper.getTableRegions("tableName");
            log.info(String.format("Number of regions: %d", regions.size()));

            for(int i=0;i<regions.size();++i){
                Pair<byte[], byte[]> pair = regions.get(i);
                String start = Bytes.toHex(pair.getFirst());
                String end = Bytes.toHex(pair.getSecond());
                properties.put(String.valueOf(i), String.format("{\"start\": %s, \"end\": %s", start, end));
            }

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