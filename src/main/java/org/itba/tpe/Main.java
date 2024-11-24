package org.itba.tpe;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

        if (args.length != 1) {
            System.err.println("Usage: Main <graphml-file>");
            System.exit(1);
        }

        Path path = new Path(args[0]);
        GraphReader reader = null;
        try {
            reader = new GraphReader(path, fs);
        } catch (IOException e) {
            System.err.println("Error reading graph: " + e.getMessage());
            System.exit(1);
        }

        Dataset<Row> vertices = reader.loadVertices(sqlContext);
        Dataset<Row> edges = reader.loadEdges(sqlContext);

        GraphFrame graph = GraphFrame.apply(vertices, edges);
        Instant currentTimestamp = Instant.now();
        String millis = String.valueOf(currentTimestamp.toEpochMilli());

        QueryProcessor queryProcessor = new QueryProcessor(graph, path.getParent(), millis, fs);
        queryProcessor.query1();
        queryProcessor.query2(sqlContext);

        sparkContext.stop();
    }
}