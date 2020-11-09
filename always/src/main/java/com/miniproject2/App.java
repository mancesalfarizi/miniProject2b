package com.miniproject2;


import java.io.File;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class App {

    public static void main( String[] args )
    {
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkSession spark =SparkSession
            .builder()
            .appName("Java Spark Hive Example")
            .config("spark.sql.warehouse.dir", warehouseLocation)
            .enableHiveSupport()
            .getOrCreate();

        spark.sql("CREATE TABLE mini (InvoiceNo string, StockCode string, description string, Quantity int, "
        + "InvoiceDate timestamp, UnitPrice float, CustomerID string, Country string) "
        + "row format delimited fields terminated by ',' "
        + "tblproperties(skip.header.line.count = 1)");

        spark.sql("LOAD DATA INPATH '/user/mancesalfarizi/data.csv' OVERWRITE INTO TABLE mini");
        Dataset<Row> semua = spark.sql("SELECT * FROM default.mini");
        App hive = new App();
        hive.barangYangSeringDibeli(semua);
        hive.berdasarkanKodeStok(semua);
        hive.berdasarkanNegara(semua);
        hive.berdasarkanTanggal(semua);
        hive.negaraPembelianTerbanyak(semua);
        hive.negaraSedikitBeli(semua);
        // spark.sql("select * from userdb.barangYangSeringDibeli limit 20").show();
        // spark.sql("select * from userdb.berdasarkanKodeStok limit 20").show();
        // spark.sql("select * from userdb.berdasarkanNegara limit 20").show();
        // spark.sql("select * from userdb.berdasarkanTanggal limit 20").show();
        // spark.sql("select * from userdb.negaraPembelianTerbanyak limit 20").show();
        // spark.sql("select * from userdb.negaraSedikitBeli limit 20").show();
    }
    private void barangYangSeringDibeli(Dataset<Row> data){
        Dataset<Row> res = data.select("Description", "Quantity")
                .groupBy("description").sum("Quantity")
                .orderBy(desc("sum(Quantity)"))
                .limit(1)
                .withColumnRenamed("sum(Quantity)", "total");

        res.write().mode("overwrite").format("orc").saveAsTable("userdb.barangYangSeringDibeli");

    }
    private void berdasarkanKodeStok(Dataset<Row> data){
        Dataset<Row> res = data.select("StockCode", "Description","Quantity")
                .groupBy("StockCode", "Description").sum("Quantity")
                .orderBy(desc("sum(Quantity)"))
                .limit(1)
                .withColumnRenamed("sum(Quantity)", "total");
                
        res.write().mode("overwrite").format("orc").saveAsTable("userdb.berdasarkanKodeStok");
    }
    private void berdasarkanNegara(Dataset<Row> data){
        Dataset<Row> res= data.select("Country", "Quantity")
                .groupBy("Country").sum("Quantity")
                .orderBy(desc("sum(Quantity)"))
                .limit(1)
                .withColumnRenamed("sum(Quantity)", "total");
                
        res.write().mode("overwrite").format("orc").saveAsTable("userdb.berdasarkanNegara");
    }
    private void berdasarkanTanggal(Dataset<Row> data){
        Dataset<Row> res = data.select("InvoiceDate", "Quantity")
                .withColumn("tanggal", to_date(col("InvoiceDate")))
                .where("StockCode = '84077'")
                .groupBy("tanggal").sum("Quantity")
                .orderBy(desc("sum(Quantity)"))
                .limit(1)
                .withColumnRenamed("sum(Quantity)", "total");
                
        res.write().mode("overwrite").format("orc").saveAsTable("userdb.berdasarkanTanggal");
    }
    
    private void negaraPembelianTerbanyak(Dataset<Row> data){
        Dataset<Row> res= data.select("Country", "Quantity")
                .where("StokCode = '8477'")
                .groupBy("StockCode").sum("Quantity")
                .orderBy(desc("sum(Quantity)"))
                .withColumnRenamed("sum(Quantity)", "total");
                
        res.write().mode("overwrite").format("orc").saveAsTable("userdb.negaraPembelianTerbanyak");
    }
    private void negaraSedikitBeli(Dataset<Row> data){
        Dataset<Row> res = data.select("Country", "Quantity")
                .groupBy("Country").sum("Quantity")
                .orderBy(asc("sum(Quantity)"))
                .withColumnRenamed("sum(Quantity)", "total");

        res.write().mode("overwrite").format("orc").saveAsTable("userdb.negaraSedikitBeli");
    }    
        
    
}