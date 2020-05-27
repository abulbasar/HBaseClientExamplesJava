#!/usr/bin/env bash


# ADMINISTRATIVE
curl --request GET http://localhost:8291/namespaces
curl --request GET http://localhost:8291/tables

curl --header "Content-Type: application/json" \
  --request POST \
  --data '{"namespace":"ns1"}' \
  http://localhost:8291/create_namespace



# DML

curl --header "Content-Type: application/json" \
  --request POST \
  --data '{"tableName":"ns1:movies", "versions": 1, "columnFamilies": ["info"]}' \
  http://localhost:8291/create_table

curl --request GET http://localhost:8291/tables?namespace=ns1

curl --request GET http://localhost:8291/describe_table?table=ns1:movies


curl --request POST http://localhost:8291/bulk_upload_movies \
        --header 'Content-Type: multipart/form-data' \
        -F file=@"/data/movielens/movies.csv"

curl --request GET http://localhost:8291/count/ns1:movies

curl --request GET http://localhost:8291/movie/1

curl --request GET http://localhost:8291/movie?q=Zulu

curl --header "Content-Type: application/json" \
  --request POST \
  --data '{"movieId": 116207, "title": "Zulu (2013) 1", "genres": "Crime|Drama|Thriller"}' \
  http://localhost:8291/movie

curl --request GET http://localhost:8291/movie/116207

curl --request POST http://localhost:8291/uploadFile \
    --header 'Content-Type: multipart/form-data' \
    -F file=@"/data/stocks.csv"


curl --requets GET http://localhost:8291/downloadFile/stocks.csv -o stocks.csv

