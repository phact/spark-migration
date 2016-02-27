package com.phact;

import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
import com.google.common.base.Objects;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.Date;

public class Join {


       /*
    CREATE TABLE support.search (
    type text,
    key text,
    author text,
    date text,
    message_number bigint,
    search_string text,
    solr_query text,
    title text,
    url text,
    PRIMARY KEY ((type, key))
    )
    */

    public static class Search implements Serializable {
        private String type;
        private String key;
        private String author;
        private Date date;
        private Long message_number;
        private String search_string;
        private String solr_query;
        private String title;
        private String url;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getAuthor() {
            return author;
        }

        public void setAuthor(String author) {
            this.author = author;
        }

        public Date getDate() {
            return date;
        }

        public void setDate(Date date) {
            this.date = date;
        }

        public Long getMessage_number() {
            return message_number;
        }

        public void setMessage_number(Long message_number) {
            this.message_number = message_number;
        }

        public String getSearch_string() {
            return search_string;
        }

        public void setSearch_string(String search_string) {
            this.search_string = search_string;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getSolr_query() {
            return solr_query;
        }

        public void setSolr_query(String solr_query) {
            this.solr_query= solr_query;
        }

        // Remember to declare no-args constructor
        public Search() { }

        public Search(String type, String key, String author, Date date, Long message_number, String search_string,String solr_query, String title, String url) {
            this.type = type;
            this.key = key;
            this.author = author;
            this.date = date;
            this.message_number = message_number;
            this.search_string = search_string;
            this.solr_query = solr_query;
            this.title = title;
            this.url = url;
        }

        // other methods, constructors, etc.
    }

    public static class Person implements Serializable {
        private Integer id;
        private String name;

        public static Person newInstance(Integer id, String name) {
            Person person = new Person();
            person.setId(id);
            person.setName(name);
            return person;
        }

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("id", id)
                    .add("name", name)
                    .toString();
        }
    }

    public Join(String keyspaceTable, String sourceIp, String destIp){
        String[] parts = keyspaceTable.split("\\.");
        String keyspace = parts[0];
        String table = parts[1];

        SparkConf conf = new SparkConf()
                .setAppName( "My application")
                .set("spark.cassandra.connection.host", sourceIp);;


        JavaSparkContext myContext = new JavaSparkContext(conf);

        CassandraTableScanJavaRDD<Search> dataRdd = CassandraJavaUtil.javaFunctions(myContext)
                .cassandraTable(keyspace, table, CassandraJavaUtil.mapRowTo(Search.class));



        conf.set("spark.cassandra.connection.host", destIp);
        CassandraConnector connectorToClusterTwo = CassandraConnector.apply(conf);

        CassandraTableScanJavaRDD<Search> dataRdd2 = CassandraJavaUtil.javaFunctions(myContext).
                cassandraTable(keyspace, table, CassandraJavaUtil.mapRowTo(Search.class));

        dataRdd2 = dataRdd2.withConnector(connectorToClusterTwo);

        System.out.println(dataRdd.count());
        System.out.println(dataRdd2.count());



    }

}
