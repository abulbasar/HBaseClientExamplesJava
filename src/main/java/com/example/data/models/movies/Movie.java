package com.example.data.models.movies;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;
import java.util.stream.Collectors;

public class Movie {
    private int movieId;
    private String title;
    private String genres;

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
        byte[] cf = Bytes.toBytes("info");
        record.addColumn(cf, Bytes.toBytes("title"), Bytes.toBytes(title));
        record.addColumn(cf, Bytes.toBytes("timestamp"), Bytes.toBytes(genres));
        return record;
    }

}
