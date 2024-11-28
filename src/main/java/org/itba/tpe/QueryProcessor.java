package org.itba.tpe;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.graphframes.GraphFrame;

import java.io.IOException;
import java.util.List;

public class QueryProcessor {
    private GraphFrame graph;
    private Path path;
    private String timestamp;
    private FileSystem fs;

    public QueryProcessor(GraphFrame graphFrame, Path path, String timestamp, FileSystem fs) {
        this.graph = graphFrame;
        this.path = path;
        this.timestamp = timestamp;
        this.fs = fs;
    }

    public void query1() throws IOException {
        GraphFrame subgraph = graph.filterVertices("labelV = 'airport'").filterEdges("labelE = 'route'");

        Dataset<Row> queryEj1 = subgraph.find("(a)-[r]->(b); (c)-[r2]->(d)")
                .filter("a.code != 'SEA' AND a.lat < 0 AND a.lon < 0 " +
                        "AND d.code = 'SEA' AND " +
                        "(b.code = c.code OR (a.code = c.code AND b.code = d.code))")
                .selectExpr("a.code AS from",
                                   "CASE WHEN b.code = 'SEA' THEN 'No Stop' ELSE b.code END AS stop",
                                   "d.code AS to")
                .distinct();


        queryEj1.show();
        JavaRDD<String> formattedRDD = queryEj1.javaRDD().map(row -> {
            String origin = row.getString(0); // Origin
            String stop = row.getString(1); // Stop
            String destination = row.getString(2); // Destination
            return String.format("%s %s %s", origin, stop, destination);
        });

        String fileName = timestamp + "-b1.txt";
        writeOutput(formattedRDD, fileName);
    }

    public void query2(SQLContext sqlContext) throws IOException {
        graph.triplets().createOrReplaceTempView("t_table");

        Dataset<Row> queryEj2 = sqlContext.sql(
                "SELECT " +
                        "    t2.src.desc AS continent, " +
                        "    (t.src.code || ' (' || t.src.desc || ')') AS country_info, " +
                        "    sort_array(collect_list(t.dst.elev)) AS elevations " +
                        "FROM t_table t " +
                        "INNER JOIN t_table t2 " +
                        "    ON t.dst.code = t2.dst.code " +
                        "WHERE t.src.labelV = 'country' " +
                        "  AND t.dst.labelV = 'airport' " +
                        "  AND t2.src.labelV = 'continent' " +
                        "  AND t2.dst.labelV = 'airport' " +
                        "GROUP BY t2.src.desc, t.src.code, t.src.desc " +
                        "ORDER BY continent, country_info"
        );

        System.out.println("Query ej B.2");
        queryEj2.show();

        JavaRDD<String> formattedRDD = queryEj2.javaRDD().map(row -> {
            String continent = row.getString(0); // Continent
            String country = row.getString(1);   // Country Code
            List<Integer> elevations = row.getList(2); // List of elevations
            String elevationList = elevations.toString();
            return String.format("%s %s %s", continent, country, elevationList);
        });

        String fileName = timestamp + "-b2.txt";
        writeOutput(formattedRDD, fileName);
    }

    private void writeOutput(JavaRDD<String> formattedRDD, String fileName) throws IOException {
        Path newPath = new Path(path, fileName);
        fs.delete(newPath, true);
        formattedRDD.saveAsTextFile(newPath.toString());
    }
}
