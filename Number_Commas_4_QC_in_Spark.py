files = ['s3://loc1/file1.csv','s3://loc2/file2.csv']

for file in files:
  rdd = sc.textFile(file).map(lambda line: (line,line.count(","))).zipWithIndex()
  df = rdd.toDF(['count','index'])
  df.printSchema()
  df.registerTempTable("df")
  df_mm = sqlContext.sql("""SELECT * FROM df WHERE count._2<>483""")
  df_mm.count()
  df_mm.collect()
  print(file)
