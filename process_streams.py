
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json
import os
import matplotlib.pyplot as plt

def process_data(rdd):
    print('-------------------Result of processed data-----------------------')
    l = rdd.collect()           #list containg string
    for s in l:
        d = json.loads(s)       #string to dict
        print(d['id'])
        write_data(d['text'], text_file)
 
        #for image
        if 'entities' in d.keys():
            if 'media' in d['entities']:
                image_url = d['entities']['media'][0]['media_url']
                write_data(image_url, image_file)

                #for video
                er = d['entities']['media'][0]['expanded_url']
                if 'video' in er:
                  write_data(er, video_file)
    tc = 0
    ic = 0
    vc = 0
    stf = 0
    sif = 0
    svf = 0
    with open(output_dir+text_file) as f:
      tc = f.read().split(separator)
      f.seek(0, os.SEEK_END)
      stf = f.tell()
    with open(output_dir+image_file) as f:
      ic = f.read().split(separator)
      f.seek(0, os.SEEK_END)
      sif = f.tell()
    with open(output_dir+video_file) as f:
      vc = f.read().split(separator)
      f.seek(0, os.SEEK_END)
      svf = f.tell()

    print("Number of tweets (text) : " + str(len(tc)-1))
    print("Number of images : " + str(len(ic)-1))
    print("Number of video : " + str(len(vc)-1))
    print("Text file size :" +str(stf) + " Bytes")
    print("Image file size :" +str(sif) + " Bytes")
    print("Video file size :" +str(svf) + " Bytes")

    #plotting the data
    # names = ['text', 'images', video]      #matplotlib is not thread safe
    # count = [tc,ic,vc]
    # plt.bar(names, count)
    # plt.show()

def write_data(data,file_name):
    if not os.path.isdir(output_dir):
      os.mkdir(output_dir)
    with open(output_dir+file_name, 'a') as f:
      f.write(data+separator)
    print('data witten')


if __name__ == '__main__':
  
  sc = SparkContext("local[2]", "Twitter")
  ssc = StreamingContext(sc,5)

  output_dir = 'output/'
  text_file = 'text_file'
  image_file = 'images'
  video_file = 'video'
  expanded_url_file = 'expanded_url'
  separator = '\n\n'

  #tracking changes in this directory
  data_dir = 'raw_data_5/'

  ds = ssc.textFileStream(data_dir)
  ds.foreachRDD(process_data)


  ssc.start()             # Start the computation
  ssc.awaitTermination()  # Wait for the computation to terminate

