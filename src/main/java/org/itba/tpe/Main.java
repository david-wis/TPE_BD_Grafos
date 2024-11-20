package org.itba.tpe;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.graphframes.GraphFrame;

import java.io.IOException;
import java.time.Instant;

public class Main {
    public static void main(String[] args) throws IOException {
        SparkConf sparkConf = new SparkConf().setAppName("TPE David Wischñevsky");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sparkContext);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        GraphReader loader = new GraphReader(args[0], fs);
        Dataset<Row> vertices = loader.loadVertices(sqlContext);
        Dataset<Row> edges = loader.loadEdges(sqlContext);

        GraphFrame graph = GraphFrame.apply(vertices, edges);

//        System.out.println("Vertices:");
//        vertices.show();
//        System.out.println("Edges:");
//        edges.show();

        Instant currentTimestamp = Instant.now();
        String millis = String.valueOf(currentTimestamp.toEpochMilli());
        String parentDirectory = args[0].substring(0, args[0].lastIndexOf("/"));

        QueryProcessor queryProcessor = new QueryProcessor(graph, parentDirectory, millis, fs);
        queryProcessor.query1();
        queryProcessor.query2(sqlContext);

        sparkContext.stop();
    }
}