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

    public QueryProcessor(GraphFrame graphFrame, String directory, String timestamp, FileSystem fs) {
        this.graph = graphFrame;
        this.path = new Path(directory);
        this.timestamp = timestamp;
        this.fs = fs;
    }

    public void query1() throws IOException {
        Dataset<Row> queryEj1Bfs2 = graph.bfs()
                .fromExpr("labelV = 'airport' AND code != 'SEA' AND lat < 0 AND lon < 0")
                .edgeFilter("labelE = 'route'")
                .toExpr("labelV = 'airport' AND code = 'SEA'")
                .maxPathLength(2)
                .run();


        queryEj1Bfs2.show();

        boolean hasV1 = queryEj1Bfs2.columns().length > 0 && java.util.Arrays.asList(queryEj1Bfs2.columns()).contains("v1");

        Dataset<Row> queryEj1;

        if (hasV1)
            queryEj1 = queryEj1Bfs2.selectExpr("from.code AS from",
                    "CASE WHEN v1 IS NULL THEN 'No Stop' ELSE v1.code END AS stop", "to.code AS to");
        else
            queryEj1 = queryEj1Bfs2.selectExpr("from.code AS from", "'No Stop' AS stop", "to.code AS to");

        queryEj1.show();

        long sinEscalas = hasV1 ? queryEj1Bfs2.filter("v1 is null").count() : queryEj1Bfs2.count();
        long conEscala = hasV1 ? queryEj1Bfs2.filter("v1 is not null").count() : 0;

        System.out.printf("Sin escalas: %d\nCon escalas: %d\n", sinEscalas, conEscala);

        System.out.println("Query ej B.1");

        queryEj1.show();
        JavaRDD<String> formattedRDD = queryEj1.javaRDD().map(row -> {
            String origin = row.getString(0); // Origin
            String stop = row.getString(1); // Stop
            String destination = row.getString(2); // Destination
            return String.format("%s\t%s\t%s", origin, stop, destination);
        });

        String fileName = timestamp + "-b1.txt";
        writeOutput(formattedRDD, fileName);
    }

    public void query2(SQLContext sqlContext) throws IOException {
        graph.triplets().createOrReplaceTempView("t_table");

        Dataset<Row> queryEj2Interm = sqlContext.sql(
                "    SELECT DISTINCT " +
                        "        t2.src.desc AS continent, " +
                        "        t.src.desc AS full_country, " +
                        "        t.src.code AS country, " +
                        "        INT(t.dst.elev) AS elev " +
                        "    FROM t_table t " +
                        "    INNER JOIN t_table t2 " +
                        "        ON t.dst.code = t2.dst.code " +
                        "    WHERE t.src.labelV = 'country' " +
                        "      AND t.dst.labelV = 'airport' " +
                        "      AND t2.src.labelV = 'continent' " +
                        "      AND t2.dst.labelV = 'airport' " +
                        "    ORDER BY elev "
        );

//        queryEj2Interm.show();
        queryEj2Interm.createOrReplaceTempView("t1");

        Dataset<Row> queryEj2 = sqlContext.sql(
                "SELECT continent, (country || ' (' || full_country || ')'), collect_list(elev) AS elevations " +
                        "FROM t1 " +
                        "GROUP BY continent, full_country, country " +
                        "ORDER BY continent, country"
        );

        System.out.println("Query ej B.2");
        queryEj2.show();

        JavaRDD<String> formattedRDD = queryEj2.javaRDD().map(row -> {
            String continent = row.getString(0); // Continent
            String country = row.getString(1);   // Country Code
            List<Integer> elevations = row.getList(2); // List of elevations
            String elevationList = elevations.toString();
            return String.format("%s\t%s\t%s", continent, country, elevationList);
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
