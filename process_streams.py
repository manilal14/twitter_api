
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext("local[2]", "Twitter")
ssc = StreamingContext(sc,1)

#tracking changes in this directory
data_dir = 'raw_data/'

lines = ssc.textFileStream(data_dir)
lines.pprint()




ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate

