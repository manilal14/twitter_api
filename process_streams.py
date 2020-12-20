
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json
import os

sc = SparkContext("local[2]", "Twitter")
ssc = StreamingContext(sc,3)

output_dir = 'output/'
text_file = 'text_file'
image_file = 'images'

def process_data(rdd):
    print('-------------------data is processing---------------')
    l = rdd.collect()           #list containg string
    
    for s in l:
        d = json.loads(s)       #string to dict
        print(d['id'])
        
        write_data(d['text'], text_file)

        # if 'entities' in d.keys():
        #     write_data(json.dumps(d['entities']), 'entities')

        #for image
        if 'entities' in d.keys():
            if 'media' in d['entities']:
                image_url = d['entities']['media'][0]['media_url']
                print(image_url)
                write_data(image_url, image_file)
                 

def write_data(data,file_name):
    if not os.path.isdir(output_dir):
      os.mkdir(output_dir)
    with open(output_dir+file_name, 'a') as f:
      f.write(data+"\n\n")
    print('data witten')


#tracking changes in this directory
data_dir = 'raw_data_3/'

ds = ssc.textFileStream(data_dir)
ds.foreachRDD(process_data)



ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate

