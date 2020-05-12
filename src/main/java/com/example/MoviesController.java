package com.example;

import com.example.hbase.HBaseHelper;
import com.example.models.GenericResponse;
import com.example.models.movies.Movie;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


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
             Table moviesTable = helper.getTable("ns1:movies");
             Object[] results = new Object[puts.size()];
             moviesTable.batch(puts, results);
             log.info("Number of records: " + results.length);
        }catch (InterruptedException | IOException ex){
            ex.printStackTrace();
            response.setStatus("Failed");
            response.setMessage(ex.getMessage());
        }
        return response;
    }

    @GetMapping("/movie/{movieId}")
    public Movie getProduct(@PathVariable int movieId) {
        log.info(String.format("movieId: %s", movieId));
        Movie record = null;
        try {
            Table table = helper.getTable("ns1:movies");
            Get get = new Get(Bytes.toBytes(movieId));
            Result result = table.get(get);
            record = Movie.parse(result);
        }catch (IOException ex){
            ex.printStackTrace();
        }
        return record;
    }

    @GetMapping("/count/{tableName}")
    public long countRecordInTable(@PathVariable String tableName) throws IOException {
        return helper.count(tableName);
    }


    @GetMapping("/movie")
    public List<Movie> searchMovie(@RequestParam("q") String q){
        log.info(String.format("q: %s", q));

        List<Movie> movies = null;
        try {
            Table table = helper.getTable("ns1:movies");

            FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            filters.addFilter(new SingleColumnValueFilter(Movie.cf, Movie.titleCol
                            , CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(q)));

            Scan scan = new Scan();
            scan.setFilter(filters);
            scan.setMaxResultSize(10);

            scan.addColumn(Movie.cf, Movie.titleCol);

            ResultScanner scanner = table.getScanner(scan);

            Stream<Result> rows = StreamSupport.stream(scanner.spliterator(), false);
            movies = rows.map(Movie::parse).collect(Collectors.toList());
            scanner.close();

        }catch (IOException ex){
            ex.printStackTrace();
        }
        return movies;
    }

    @PostMapping("/movie")
    public GenericResponse saveMovie(@RequestBody Movie movie){

        GenericResponse response = new GenericResponse();

        System.out.println(movie.toJson());

        Table table = helper.getTable("ns1:movies");
        try{
            table.put(movie.toPut());
        }catch (IOException ex){
            log.error(ex.getMessage(), ex);
            response.setMessage(ex.getMessage());
            response.setStatus("Failed");
        }

        return response;

    }



}