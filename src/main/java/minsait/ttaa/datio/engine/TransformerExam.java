package minsait.ttaa.datio.engine;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.jetbrains.annotations.NotNull;

import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.naming.PlayerInput.*;
import static minsait.ttaa.datio.common.naming.PlayerOutput.*;
import static org.apache.spark.sql.functions.*;

public class TransformerExam extends Writer{

    private SparkSession spark;

    public TransformerExam(@NotNull SparkSession spark) {
        this.spark = spark;
        Dataset<Row> df = readInput();

        df = cleanData(df);

        df = excerciseOneSelection(df);
        df = excersiceTwoColumn(df);
        df = excersiceThreeRowNumber(df);
        df = excersiceFourColumnPotenital(df);

        //df = excersiceFiveFilters(df);

        excersiceWrite(df);
    }

    private Dataset<Row> readInput() {
        Dataset<Row> df = spark.read()
                .option(HEADER, true)
                .option(INFER_SCHEMA, true)
                .csv(INPUT_PATH);
        return df;
    }

    /**
     * @param df
     * @return a Dataset with filter transformation applied
     * column team_position != null && column short_name != null && column overall != null
     */
    private Dataset<Row> cleanData(Dataset<Row> df) {
        df = df.filter(
                teamPosition.column().isNotNull().and(
                        shortName.column().isNotNull()
                ).and(
                        overall.column().isNotNull()
                )
        );

        return df;
    }

    private Dataset<Row> excerciseOneSelection(Dataset<Row> df) {
        return df.select(
                shortName.column(),
                longName.column(),
                age.column(),
                heightCm.column(),
                weightKg.column(),
                nationality.column(),
                clubName.column(),
                overall.column(),
                potential.column(),
                teamPosition.column()
        );
    }

    private Dataset<Row> excersiceTwoColumn(Dataset<Row> df){
        Column rule = when(age.column().$less(23), "A")
                .when(age.column().$greater$eq(23).and(age.column().$less(27)), "B")
                .when(age.column().$greater$eq(27).and(age.column().$less(32)), "C")
                .when(age.column().$greater$eq(32), "D");

        return df.withColumn(ageRange.getName(), rule);
    }

    private Dataset<Row> excersiceThreeRowNumber(Dataset<Row> df){
        WindowSpec w = Window
                .partitionBy(nationality.column(), teamPosition.column())
                .orderBy(overall.column().desc());

        Column rank = row_number().over(w);

        return df.withColumn(rankByNationalityPosition.getName(), rank);
    }

    private Dataset<Row> excersiceFourColumnPotenital(Dataset<Row> df){
        return df.withColumn(potentialVsOverall.getName(), potential.column().divide(overall.column()));
    }

    private Dataset<Row> excersiceFiveFilters(Dataset<Row> df){
       return df.filter(
               rankByNationalityPosition.column().$less(3)
        ).filter(
               ageRange.column().equalTo("B").or(ageRange.column().equalTo("C")).and(potentialVsOverall.column().$greater(1.15))
       ).filter(
               ageRange.column().equalTo("A").and(potentialVsOverall.column().$greater$eq(1.25))
       ).filter(
               ageRange.column().equalTo("D").and(rankByNationalityPosition.column().$less$eq(5))
       );
    }

}
