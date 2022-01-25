

### Rename columns

usually used before coalesce columns from 2 tables.
```scala
def renameCols(oldColumns: Seq[String], surfix: String): Seq[org.apache.spark.sql.Column] = {
    val newColumns = oldColumns.zipWithIndex.map(c => s"${c._1}_${surfix}")
    val columnsList = oldColumns.zip(newColumns).map(f=>{col(f._1).as(f._2)})
    columnsList
}
```

### coalesce columns and drop

usually used before coalesce columns from 2 tables.
```scala
val columnNames = Seq("wtype","date","language","ctype","clength","doc", "uri")

val ACols = renameCols(columnNames, "a") :+ col("key_col")
val BCols = renameCols(columnNames, "b") :+ col("key_col")

val ADF = spark.read.parquet(...).alias("A").select(ACols:_*)
val BDF = spark.read.parquet(...).alias("B").select(BCols:_*)

val joinedDF = BDF.join(ADF, Seq("key_col"),"fullouter")

val resultDF = columnNames.foldLeft(joinedDF) {
    (tmpDf, c) =>
        tmpDf.withColumn(c, coalesce(col(s"${c}_a"), col(s"${c}_b")))
        .drop(s"${c}_a").drop(s"${c}_b")
}
```
