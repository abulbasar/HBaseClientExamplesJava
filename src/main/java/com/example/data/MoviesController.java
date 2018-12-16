package com.example.data;

import com.example.CsvUtils;
import com.example.data.hbase.HBaseHelper;
import com.example.data.models.GenericResponse;
import com.example.data.models.admin.CreateNamespaceRequest;
import com.example.data.models.admin.CreateProductsRequest;
import com.example.data.models.admin.CreateTableRequest;
import com.example.data.models.ProductRequest;
import com.example.data.models.movies.Movie;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


@RestController
public class MoviesController {

    private static final Logger log = LoggerFactory.getLogger(MoviesController.class.getName());

    private static HBaseHelper helper;

    public MoviesController(){
        helper = HBaseHelper.getInstance();
    }

    @PostMapping(value = "/bulk_upload_movies", consumes = "multipart/form-data")
    public GenericResponse bulkMoviesUpload(@RequestParam("file") MultipartFile request) {
        log.info(String.format("Request body size: %s", request.getSize()));
        GenericResponse response = new GenericResponse();
        try {
             List<Movie> rows = CsvUtils.read(Movie.class, request.getInputStream());
             List<Put> puts = rows.stream().map(r -> r.toPut()).collect(Collectors.toList());
             helper.getTable("ns1:movies").put(puts);
             rows.stream().forEach(v -> log.debug(v.getTitle()));
        }catch (IOException ex){
            ex.printStackTrace();
            response.setStatus("Failed");
            response.setMessage(ex.getMessage());
        }
        return response;
    }

    @GetMapping("/product/{rowId}")
    public boolean getProduct(@PathVariable String rowId) {
        log.info(String.format("Product Id: %s", rowId));
        try {
            Table table = helper.getTable("n1:product");
            Get get = new Get(Bytes.toBytes(rowId));
            Result result = table.get(get);
        }catch (IOException ex){
            ex.printStackTrace();
        }
        return true;
    }



}