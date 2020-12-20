# -*- coding: utf-8 -*-

#for importing twitter_cred
import sys
sys.path.append('/home/mani/Desktop/sai/')
#create this file to hode all the neccessary keys from twiier 
import twitter_cred

import tweepy
import socket
import os
import json
import time


auth = tweepy.OAuthHandler(twitter_cred.api_key, twitter_cred.api_key_secret)
auth.set_access_token(twitter_cred.access_token, twitter_cred.access_token_secret)
api = tweepy.API(auth)

class MyStreamListener(tweepy.StreamListener):
 
  def on_status(self, data):
    data = data._json   #dict
    print(data['id'])
    self.add_to_file(data)

  def add_to_file(self,tweet):
    data_dir = 'raw_data_3/'
    file_name = tweet['id']
    if not os.path.isdir(data_dir):
      os.mkdir(data_dir)
    with open(data_dir+str(file_name), 'a') as f:
      f.write(json.dumps(tweet)+"\n")
    print('data dumped')


  def on_error(self, status):
    print("Error "+str(status))
    if status == 420:
      return False
    return True

myStreamListener =  MyStreamListener()
myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener, tweet_mode="extended_tweet")
myStream.filter(track=['covid'], languages=['en'])
