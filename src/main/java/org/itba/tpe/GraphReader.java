package org.itba.tpe;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GraphReader {
    private final Graph graph;

    public GraphReader(String path, FileSystem fs) throws IOException {
        this.graph = new TinkerGraph();
        try (FSDataInputStream fis = fs.open(new Path(path))) {
            GraphMLReader reader = new GraphMLReader(graph);
            reader.inputGraph(fis);
        }
    }

    public Dataset<Row> loadVertices(SQLContext sqlContext) throws IOException {
        List<Row> rows = new ArrayList<>();
        graph.getVertices().forEach(v -> {
            rows.add(RowFactory.create(
                    v.getId().toString(),
                    v.getProperty("type"),
                    v.getProperty("code"),
                    v.getProperty("icao"),
                    v.getProperty("desc"),
                    v.getProperty("region"),
                    v.getProperty("runways") != null ? Integer.parseInt(v.getProperty("runways").toString()) : null,
                    v.getProperty("longest") != null ? Integer.parseInt(v.getProperty("longest").toString()) : null,
                    v.getProperty("elev") != null ? Integer.parseInt(v.getProperty("elev").toString()) : null,
                    v.getProperty("country"),
                    v.getProperty("city"),
                    v.getProperty("lat") != null ? Double.parseDouble(v.getProperty("lat").toString()) : null,
                    v.getProperty("lon") != null ? Double.parseDouble(v.getProperty("lon").toString()) : null,
                    v.getProperty("author"),
                    v.getProperty("date"),
                    v.getProperty("labelV")
            ));
        });

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.StringType, false),
                DataTypes.createStructField("type", DataTypes.StringType, true),
                DataTypes.createStructField("code", DataTypes.StringType, true),
                DataTypes.createStructField("icao", DataTypes.StringType, true),
                DataTypes.createStructField("desc", DataTypes.StringType, true),
                DataTypes.createStructField("region", DataTypes.StringType, true),
                DataTypes.createStructField("runways", DataTypes.IntegerType, true),
                DataTypes.createStructField("longest", DataTypes.IntegerType, true),
                DataTypes.createStructField("elev", DataTypes.IntegerType, true),
                DataTypes.createStructField("country", DataTypes.StringType, true),
                DataTypes.createStructField("city", DataTypes.StringType, true),
                DataTypes.createStructField("lat", DataTypes.DoubleType, true),
                DataTypes.createStructField("lon", DataTypes.DoubleType, true),
                DataTypes.createStructField("author", DataTypes.StringType, true),
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("labelV", DataTypes.StringType, true)
        });

        return sqlContext.createDataFrame(rows, schema);
    }

    public Dataset<Row> loadEdges(SQLContext sqlContext) {
        List<Row> rows = new ArrayList<>();
        graph.getEdges().forEach(e -> {
            rows.add(RowFactory.create(
                    e.getVertex(com.tinkerpop.blueprints.Direction.OUT).getId().toString(),
                    e.getVertex(com.tinkerpop.blueprints.Direction.IN).getId().toString(),
                    e.getProperty("dist") != null ? Integer.parseInt(e.getProperty("dist").toString()) : null,
                    e.getProperty("labelE")
            ));
        });

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("src", DataTypes.StringType, false),
                DataTypes.createStructField("dst", DataTypes.StringType, false),
                DataTypes.createStructField("dist", DataTypes.IntegerType, true),
                DataTypes.createStructField("labelE", DataTypes.StringType, true)
        });

        return sqlContext.createDataFrame(rows, schema);
    }
}
