package com.examplebank.bigdataefthavale.rdtomongo;

import com.mongodb.spark.MongoSpark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class Application {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("PostgreSQL to MongoDB")
                .config("spark.mongodb.output.uri","mongodb://52.246.250.73/dwh.eft")
                .getOrCreate();

        Dataset<Row> load = sparkSession.read().format("jdbc")
                .option("driver", "org.postgresql.Driver")
                .option("url", "jdbc:postgresql://52.246.250.73/finance")
                .option("dbtable", "public.eftdata")
                .option("user", "postgres")
                .option("password", "12345").load();

        Dataset<Row> dataset = load.withColumn("balance", load.col("balance").cast(DataTypes.IntegerType));

        MongoSpark.write(load).mode("append").save();

    }
}
