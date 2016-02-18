package com.phact;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.cassandra.CassandraSQLContext;

import java.io.Serializable;

public class Migrate implements Serializable{

    public Migrate(){

        SparkConf conf = new SparkConf()
                .setAppName( "My application");
                //.forDse;

        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLocalProperty("spark.cassandra.connection.host", "127.0.0.1");
       
        CassandraConnector connectorToClusterOne = CassandraConnector.apply(jsc.getConf());
       
        try(Session session1 = connectorToClusterOne.openSession() ){
             session1.execute("SELECT * FROM system.peers");
        }

        JavaRDD<String> stringRdd = CassandraJavaUtil.javaFunctions(jsc)
                .cassandraTable("system", "peers", CassandraJavaUtil.mapColumnTo(String.class))
                .select("peer");
        
        CassandraSQLContext sqlContext1 = new CassandraSQLContext(jsc.sc());
        DataFrame peers1 = sqlContext1.sql("select * from system.peers");

        long count1 = peers1.count();
        System.out.println(count1);

        jsc.setLocalProperty("spark.cassandra.connection.host", "172.31.21.193");
        
        CassandraConnector connectorToClusterTwo = CassandraConnector.apply(jsc.getConf());

        try(Session session2 = connectorToClusterTwo.openSession() ){
             session2.execute("SELECT * FROM system.peers");
        }
 
        JavaRDD<String> stringRdd2 = CassandraJavaUtil.javaFunctions(jsc)
                .cassandraTable("system", "peers", CassandraJavaUtil.mapColumnTo(String.class))
                .select("peer");

        
        CassandraSQLContext sqlContext2 = new CassandraSQLContext(jsc.sc());
        DataFrame peers2 = sqlContext2.sql("select * from system.peers");



        long count2 = peers2.count();
        System.out.println(count2);

    }
}
