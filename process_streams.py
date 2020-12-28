
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json
import os
import time

def write_data(data,file_name):
    if not os.path.isdir(output_dir):
      os.mkdir(output_dir)
    with open(output_dir+file_name, 'a') as f:
      f.write(data+separator)

def getCount(file_name):
  count = byt = 0
  if os.path.isfile(output_dir+file_name):
    with open(output_dir+file_name) as f:
      count = len(f.read().split(separator))-1
      f.seek(0, os.SEEK_END)
      byt = f.tell()
  return (count, byt)

def process_data(rdd):
    global last_tweet_at, last_tweet_id
    print('\n-------------------Result of processed data (Every 10 sec)-----------------------\n')
    l = rdd.collect()           #list containg string
    print("count of rdd (current batch ) : " + str(rdd.count()))
    
    for s in l:
        d = json.loads(s)       #string to dict
        #print(d['id'])
        last_tweet_at = d['created_at']
        last_tweet_id = d['id']

        write_data(str(d['id']), id_file)
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

    tc = ic = vc = idc = tb = ib = vb = idb = 0

    if os.path.isdir(output_dir):
      tc, tb = getCount(text_file)
      ic, ib = getCount(image_file)
      vc, vb = getCount(video_file)
      idc,idb = getCount(id_file)
          
    print("Total number of tweets : " + str(idc))
    print("Total number of images : " + str(ic))
    print("Total number of videos : " + str(vc))
    #print("Number of id : " + str(len(id)-1))
    print("Text file size : " +str(tb) + " Bytes")
    print("Image file size : " +str(ib) + " Bytes")
    print("Video file size : " +str(vb) + " Bytes")
    print("Last tweet at : " + last_tweet_at + " with tweet id : " + str(last_tweet_id))

    #plotting the data
    # names = ['text', 'images', video]      #matplotlib is not thread safe
    # count = [tc,ic,vc]
    # plt.bar(names, count)
    # plt.show()

    #time.sleep(5)


if __name__ == '__main__':
  
  sc = SparkContext("local[2]", "Twitter")
  ssc = StreamingContext(sc,10)

  ssc.checkpoint('checkpoint')

  output_dir = 'output_2/'
  text_file = 'text_file'
  image_file = 'images'
  video_file = 'video'
  id_file = 'id_file'
  expanded_url_file = 'expanded_url'
  separator = '\n\n'

  last_tweet_at = "Not Available"
  last_tweet_id = -1

  #tracking changes in this directory
  data_dir = 'raw_data_6/'

  ds = ssc.textFileStream(data_dir)
  ds.foreachRDD(process_data)


  ssc.start()             # Start the computation
  ssc.awaitTermination()  # Wait for the computation to terminate

