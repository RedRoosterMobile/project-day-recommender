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
# spark-submit --master local[2] --total-executor-cores 4 --executor-memory 4g server.py
spark-submit --master local[4] --driver-memory 4g --executor-memory 4g server.py


# Note that we run with local[2, meaning two threads - which represents "minimal" parallelism,
# which can help detect bugs that only exist when we run in a distributed context.



# scala examples from spark download
# park-submit --class org.apache.spark.examples.mllib.RecommendationExample --master local[2] ./examples/jars/spark-examples_2.11-2.0.1.jar
# park-submit --class org.apache.spark.examples.mllib.JavaRecommendationExample --master local[2] ./examples/jars/spark-examples_2.11-2.0.1.jar
