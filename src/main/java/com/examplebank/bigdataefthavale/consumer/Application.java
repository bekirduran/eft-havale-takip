package com.examplebank.bigdataefthavale.consumer;


import com.mongodb.spark.MongoSpark;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class Application {
    public static void main(String[] args) throws StreamingQueryException {
        System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master");
        SparkSession sparkSession = SparkSession.builder().master("local")
                .config("spark.mongodb.output.uri", "mongodb://52.246.250.73/financeDB.traceeft")
                .appName("EFT Tracer").getOrCreate();

        StructType accountType = new StructType()
                .add("iban", DataTypes.StringType)
                .add("fullname", DataTypes.StringType)
                .add("oid", DataTypes.LongType);

        StructType infoType = new StructType()
                .add("bank", DataTypes.StringType)
                .add("iban", DataTypes.StringType)
                .add("fullname", DataTypes.StringType);


        StructType type = new StructType()
                .add("balance", DataTypes.LongType)
                .add("btype", DataTypes.StringType)
                .add("ptype", DataTypes.StringType)
                .add("pid", DataTypes.LongType)
                .add("time", DataTypes.TimestampType)
                .add("account", accountType)
                .add("info", infoType);

        Dataset<Row> rowDataset = sparkSession.readStream().format("kafka")
                .option("kafka.bootstrap.servers", "52.246.250.73:9092")
                .option("subscribe", "trace-eft").load().selectExpr("CAST (value as STRING)");

        Dataset<Row> df = rowDataset.select(functions.from_json(rowDataset.col("value"), type).as("data")).select("data.*");

        //  Dataset<Row> processVolume = df.groupBy("btype").sum("balance");

        Dataset<Row> timeWindowDf = df.groupBy(functions.window(df.col("time"), "4 hour"), df.col("btype")).sum("balance");

        Dataset<Row> allDf = df.select("balance", "btype", "ptype", "pid", "time", "account.*", "info");

        Dataset<Row> renamed = allDf.withColumnRenamed("iban", "from_iban").withColumnRenamed("fullname", "from_fullname");
        Dataset<Row> csvOutput = renamed.select("balance", "btype", "ptype", "pid", "time", "from_iban", "from_fullname", "info.*");

        // Writing csv
        //    csvOutput.coalesce(1).write().option("header",true).option("encoding", "UTF-8").csv("d:\\eftdata.csv");

        //Writing mongoDb
        timeWindowDf.writeStream().outputMode("complete").foreachBatch((VoidFunction2<Dataset<Row>, Long>) (rowDataset1, aLong) ->
                MongoSpark.write(rowDataset1).mode("append").save()
        ).start().awaitTermination();

    }
}
