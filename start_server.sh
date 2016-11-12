# TODO:
# - check your path to spark
# - the --master parameters must point to your Spark cluster setup (can be local).
# - see: https://spark.apache.org/docs/latest/submitting-applications.html
# - check the total number of your cpu cores
# - check your total RAM
~/spark-2.0.1-bin-hadoop2.7/bin/spark-submit --master spark://ec2-35-163-204-94.us-west-2.compute.amazonaws.com:7077 --total-executor-cores 4 --executor-memory 2g --py-files Archive.zip server.py
