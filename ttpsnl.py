#!/usr/bin/env python
# Copyright 2015 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""This script uses the Twitter Streaming API, via the tweepy library,
to pull in tweets and publish them to a PubSub topic.
"""

import base64
import datetime
import os
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import json
import utils

from google.cloud import language
from google.cloud.language import enums
from google.cloud.language import types
from google.api_core.exceptions import InvalidArgument

# Get your twitter credentials from the environment variables.
# These are set in the 'twitter-stream.json' manifest file.
consumer_key = '******************************'
consumer_secret = '******************************'
access_token = '******************************'
access_token_secret = '******************************'

PUBSUB_TOPIC = 'projects/*******/topics/socialmedia-demo-topic'
NUM_RETRIES = 3


def publish(client, pubsub_topic, data_lines):
    """Publish to the given pubsub topic."""
    print 'publish'
    messages = []
    for line in data_lines:
        pub = base64.urlsafe_b64encode(line)
        messages.append({'data': pub})
        print line
    body = {'messages': messages}
    resp = client.projects().topics().publish(
            topic=pubsub_topic, body=body).execute(num_retries=NUM_RETRIES)
    return resp


class StdOutListener(StreamListener):
    """A listener handles tweets that are received from the stream.
    This listener dumps the tweets into a PubSub topic
    """

    count = 0
    twstring = ''
    tweets = []
    batch_size = 50
    total_tweets = 10000000
    client = utils.create_pubsub_client(utils.get_credentials())
    print 'in stdoutlistener'

    def write_to_pubsub(self, tw):
        publish(self.client, PUBSUB_TOPIC, tw)

    def on_data(self, data):
        """What to do when tweet data is received."""
        client = language.LanguageServiceClient()

        pub_data = {}

        all_data = json.loads(data)
        pub_data["created_at"] =all_data["created_at"]
        pub_data["tweet"] = all_data["text"]
        pub_data["username"] = all_data["user"]["screen_name"]
        pub_data["userlocation"] = all_data["user"]["location"]
        pub_data["retweetcount"] = all_data["retweet_count"]
        pub_data["favoritecount"] = all_data["favorite_count"]
        pub_data["profileimage_url"] = all_data["user"]["profile_image_url_https"]
        pub_data["replycount"] = all_data["reply_count"]

      
        document = types.Document(content=pub_data["tweet"],type=enums.Document.Type.PLAIN_TEXT)
        try:
          sentiment = client.analyze_sentiment(document=document).document_sentiment
          pub_data["score"] = sentiment.score
          pub_data["magnitude"] = sentiment.magnitude
          entities = client.analyze_entities(document).entities
          mention_count = 1
          for entity in entities:
            if mention_count == 1: pub_data["mention_1"] = entity.name
            elif mention_count == 2: pub_data["mention_2"] = entity.name
            elif mention_count == 3: pub_data["mention_3"] = entity.name
            elif mention_count == 4: pub_data["mention_4"] = entity.name
            elif mention_count == 5: pub_data["mention_5"] = entity.name
            else: pass
            mention_count = mention_count + 1 
            print(u'{:<16}: {}'.format('name', entity.name))
          print('Sentiment: {}, {}'.format(sentiment.score, sentiment.magnitude))
        except InvalidArgument as e:
          pub_data["score"] =  0
          pub_data["magnitude"] = 0
          print "NLP API Bypassed"

        print "Tweet" , pub_data["tweet"]

        pass_data = json.dumps(pub_data)

        self.tweets.append(pass_data)
        if len(self.tweets) >= self.batch_size:
            self.write_to_pubsub(self.tweets)
            self.tweets = []
        self.count += 1
 
        # if we've grabbed more than total_tweets tweets, exit the script.
        if self.count > self.total_tweets:
            return False
        if (self.count % 1000) == 0:
            print 'count is: %s at %s' % (self.count, datetime.datetime.now())
        return True

    def on_error(self, status):
        print status


if __name__ == '__main__':
    print '....'
    listener = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, listener)
    stream.filter(track=['technology','#googlecloudonboard','#GoogleCloudOnboard', '#GOOGLECloudOnboard'])
