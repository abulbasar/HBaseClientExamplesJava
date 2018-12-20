package com.example.models.movies;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;



@JsonInclude(JsonInclude.Include.NON_NULL)
public class Movie {
    private int movieId;
    private String title;
    private String genres;

    public final static byte[] cf = Bytes.toBytes("info");
    public final static byte[] titleCol = Bytes.toBytes("title");
    public final static byte[] genresCol = Bytes.toBytes("genres");



    public Movie(){}


    public int getMovieId() {
        return movieId;
    }

    public void setMovieId(int movieId) {
        this.movieId = movieId;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getGenres() {
        return genres;
    }

    public void setGenres(String genres) {
        this.genres = genres;
    }

    public Put toPut(){
        Put record = new Put(Bytes.toBytes(movieId));
        if(title != null){
            record.addColumn(cf, titleCol, Bytes.toBytes(title));
        }
        if(genres != null){
            record.addColumn(cf, genresCol, Bytes.toBytes(genres));
        }
        return record;
    }

    public static Movie parse(Result result){
        Movie record = new Movie();
        record.setMovieId(Bytes.toInt(result.getRow()));
        record.setTitle(Bytes.toString(result.getValue(cf, titleCol)));
        record.setGenres(Bytes.toString(result.getValue(cf, genresCol)));
        return record;
    }

    public String toJson(){
        ObjectMapper mapper = new ObjectMapper();
        String json = null;
        try {
            json = mapper.writeValueAsString(this);
        }catch (JsonProcessingException ex){
            ex.printStackTrace();
        }

        return json;

    }

}
