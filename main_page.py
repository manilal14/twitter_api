import tweepy
import twitter_cred
import json
import os.path

class Authentication():
    
    def get_api(self):
        auth = tweepy.OAuthHandler(twitter_cred.api_key, twitter_cred.api_key_secret)
        auth.set_access_token(twitter_cred.access_token, twitter_cred.access_token_secret)
        return tweepy.API(auth, parser=tweepy.parsers.JSONParser())

class MyTweets():
    
    def __init__(self, api):
        self.api = api
        self.data_file = 'data.json'
        
    
    def get_my_custome_tweets(self, count):
        
        tweets = self.api.home_timeline(count = count, tweet_mode='extended')
        
        tweets_list = []
        
        for t in tweets:
            text = t['full_text']
            tweet_url = "https://twitter.com/twitter/statuses/" + t['id_str']
            
            try:
                media_url = t['entities']['media'][0]['media_url']
            except KeyError:
                media_url = ""
            
            data = {
                    "text":text,
                    "tweet_url" : tweet_url,
                    "media_url":media_url
                    }
            
            tweets_list.append(data)
        
        return tweets_list
    

    def get_old_tweets(self):
        old_tweets = []
        if os.path.exists(self.data_file):
            with open(self.data_file, 'r') as f:
                old_tweets = json.load(f)
        return old_tweets
        
    
    def add_tweets_to_file(self, new_tweets):
        
        old_tweets = self.get_old_tweets()
        
        with open(self.data_file, 'w') as f:
            json_data = json.dumps(old_tweets + new_tweets)
            f.write(json_data)
       
    
if __name__ =="__main__":
    
    api = Authentication().get_api()
    
    my_tweets = MyTweets(api)
    
    tweets_list = my_tweets.get_my_custome_tweets(2)
    for t in tweets_list:
        print(t['text'])
        
    my_tweets.add_tweets_to_file(tweets_list)
    
    
    
   
    
    
    
        
    

    
    

