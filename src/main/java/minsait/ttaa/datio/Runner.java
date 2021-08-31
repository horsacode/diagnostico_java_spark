package minsait.ttaa.datio;

import minsait.ttaa.datio.engine.Transformer;
import minsait.ttaa.datio.engine.TransformerExam;
import org.apache.spark.sql.SparkSession;

import static minsait.ttaa.datio.common.Common.SPARK_MODE;

public class Runner {
    static SparkSession spark = SparkSession
            .builder()
            .master(SPARK_MODE)
            .getOrCreate();

    public static void main(String[] args) {
        //Transformer engine = new Transformer(spark);
        TransformerExam engine = new TransformerExam(spark);
    }
}
