import org.apache.spark.sql.DataFrame

def anova_table( df: DataFrame, categ_col_name: String, label_col_name:String ):Unit = {
  val categcol = col(categ_col_name)
  val labelcol = col(label_col_name)

  import org.apache.spark.sql.expressions.Window

  val overall = Window.partitionBy()
  val ss_total = df.withColumn("overall_mean",mean(labelcol).over(overall)).select(sum(pow(labelcol-col("overall_mean"),2)).as("SS_total")).first.getDouble(0)

  val categwindow = Window.partitionBy(categcol)
  val ss_within = df.withColumn("mean_within",mean(labelcol).over(categwindow)).select(sum(pow(labelcol-col("mean_within"),2)).as("SS_within")).first.getDouble(0)

  val ss_between = df.withColumn("overall_mean",mean(labelcol).over(overall)).withColumn("mean_within",mean(labelcol).over(categwindow)).select(sum(pow(col("mean_within")-col("overall_mean"),2)).as("SS_between")).first.getDouble(0)

  println(label_col_name + ":")
  println("ss_total=" + ss_total + ", ss_within=" + ss_within + ", ss_between=" + ss_between)

  val observation_size = df.count
  val k = df.dropDuplicates(categ_col_name).count

  val df_total = observation_size - 1
  val df_within = observation_size - k
  val df_between = k - 1

  println("df_total=" + df_total + ", df_within=" + df_within + ", df_between=" + df_between)

  val mean_sq_between = ss_between / (k - 1)
  val mean_sq_within = ss_within / (observation_size - k)
  val F = mean_sq_between / mean_sq_within

  import org.apache.commons.math3.distribution.FDistribution

  val fdist = new FDistribution(null, df_between, df_within)

  println("sum_sq=" + ss_between + ", df=" + df_between + ", F=" + F + ", PR(>F)=" + (1 - fdist.cumulativeProbability(F)))
  println
}
