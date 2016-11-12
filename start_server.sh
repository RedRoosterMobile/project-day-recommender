# TODO:
# - check your path to spark
# - the --master parameters must point to your Spark cluster setup (can be local).
# - see: https://spark.apache.org/docs/latest/submitting-applications.html
# - check the total number of your cpu cores
# - check your total RAM

# manually installed and build with sbt on linux/mac
#~/spark-2.0.1-bin-hadoop2.7/bin/spark-submit --total-executor-cores 4 --executor-memory 2g server.py

# via homebrew
# TODO: org/apache/spark/log4j-defaults.properties adjust this make logging better
spark-submit --total-executor-cores 4 --executor-memory 2g server.py
