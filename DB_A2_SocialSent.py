
import tweepy
import csv
import re
import nltk
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import time
import json

#nltk.download('stopwords')
#nltk.download('punkt')
#nltk.download('averaged_perceptron_tagger')

def getTweetAPI():
    consumer_key = "GG1MmGFXWbVEvjAz5thB5EQDs"
    consumer_secret = "NG0nsSsy0Iu29RKVr2z3hSiL4HcwcHievXfE8Qw4r6x77AdPd0"
    access_key = "1002349562093363200-O5m7LI30kIMuruS9U2tCs06zza2711"
    access_secret = "i0o8KPVxU6pIRcrmzW60vmpJ1CbS6oib9IXDt28tgpqXP"

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True)
    return api

def cleanText(original_text):
    text = original_text.replace('\n', ' ')
    # remove bad unicode
    text = re.sub('[^(\x20-\x7F)]+', ' ', text)

    # remove https
    text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', " ", text)
    text = text.strip()
    text = text.lower()
    text = text.replace("black", "")
    text = text.replace("panther", "")

    text = text.split()
    text = ' '.join(text)

    return text

def cleanTextForES(original_text):

    text = original_text.replace('\n', ' ')
    # remove bad unicode
    text = re.sub('[^(\x20-\x7F)]+', ' ', text)
    text = text.strip()
    return text

def load_sentiWordNet(pathname):
    tempDictionary = {}
    sentiDict = {}
    for line in open(pathname):
        line = line.strip()
        if line.startswith("#") == False :
            data = line.split("\t")
            wordTypeMarker = data[0]
            synTermsSplit = data[4].split(" ")
            for synTermSplit  in synTermsSplit:
                synTermAndRank = synTermSplit.split("#")
                synTerm = synTermAndRank[0] + "#" + wordTypeMarker
                synTermRank = int(synTermAndRank[1])

                if tempDictionary.has_key(synTerm) == False:
                    tempDictionary[synTerm] = {}

                posScore = float(data[2])
                negScore = float(data[3])
                neturalScore = 1 - posScore - negScore
                tempDictionary[synTerm][synTermRank] = {}
                tempDictionary[synTerm][synTermRank]["posScore"] = posScore
                tempDictionary[synTerm][synTermRank]["negScore"] = negScore
                tempDictionary[synTerm][synTermRank]["neturalScore"] = neturalScore

    for (word, ScoreMap) in tempDictionary.items():
        #print "dict[%s]=" % k, v
        posScore = 0.0;
        negScore = 0.0;
        sum = 0.0
        for (rank, score) in ScoreMap.items():
            posScore = posScore + score["posScore"]/(rank*1.0)
            negScore = negScore + score["negScore"]/(rank*1.0)
            sum = sum + 1.0 / (rank*1.0)

        posScore = posScore / sum
        negScore = negScore / sum
        sentiDict[word] = {}
        sentiDict[word]["posScore"]  = posScore
        sentiDict[word]["negScore"]  = negScore
        sentiDict[word]["neturalScore"] = 1 - (posScore + negScore)

    tempDictionary.clear()
    tempDictionary = None

    return sentiDict

def load_socialSent(pathName):
    load_dict = {}
    with open(pathName, "r") as load_f:
        load_dict = json.load(load_f)
        return load_dict


suffix = time.strftime("%Y%m%d_%H_%M_%S", time.localtime())

api = getTweetAPI()
query = "black panther -filter:retweets"
sentiDict = load_socialSent("twitter-scores.json")

#RB  (Adverbs) r
#JJ (Adjectives) a
#NN (Common Nouns) n
#VB (Verb base forms) v


with open('original_tweet_data'+suffix+".csv", 'wb') as original_tweet_file:
    with open('sentiment_tweet_data'+suffix+".csv", 'wb') as senti_tweet_file:
        original_writer = csv.writer(original_tweet_file)
        original_writer.writerow(['id', 'user', 'created_at', 'text'])

        senti_writer = csv.writer(senti_tweet_file)
        senti_writer.writerow(['id', 'user', 'created_at', 'text', 'sentiment', 'score'])

        tweetCollect = tweepy.Cursor(api.search, q=query, lang='en', count=100).items()
        count = 0
        for tweet in tweetCollect:

            original_text = tweet.text.encode('utf8')
            original_writer.writerow([tweet.id_str, tweet.user.screen_name, tweet.created_at, original_text])

            clean_text = cleanText(original_text)
            text_tokens = nltk.word_tokenize(clean_text)

            final_score = valid_word_count = 0
            for word in text_tokens:
                if sentiDict.has_key(word):
                    valid_word_count += 1
                    final_score += sentiDict[word]
                elif sentiDict.has_key("#"+word):
                    valid_word_count += 1
                    final_score += sentiDict["#"+word]

            if valid_word_count > 0:
                final_score = final_score*1.0 / valid_word_count

                if final_score > 0 :
                    sentiment = "Positive"
                elif final_score < 0:
                    sentiment = "Negative"
                else:
                    final_score = 0
                    sentiment = "Neutral"

            else:
                final_score = 0
                sentiment = "Neutural"

            temp_text = original_text.replace('\n', ' ')
            senti_writer.writerow([tweet.id_str, tweet.user.screen_name, tweet.created_at, temp_text , sentiment, round(final_score,2)])
            count = count + 1
            if count == 200:
                break

original_tweet_file.close();
senti_tweet_file.close();
sentiDict.clear()
sentiDict = None

#es = Elasticsearch(['http://35.183.6.252:9200/'])
es = Elasticsearch()
i = 0
actions = []
with open("sentiment_tweet_data"+suffix+".csv", 'rb') as csvfile:
    reader = csv.reader(csvfile)
    for item in reader:
        if reader.line_num == 1:
            continue
        text = cleanTextForES(item[3])
        record = {
            "_index": "tweet_data",
            "_type": "_doc",
            "_source": {
                "id": item[0],
                "user": item[1],
                "created_at": item[2],
                "text": text,
                "sentiment": item[4],
                "score": item[5]
            }
        }
        actions.append(record.copy())
        i = i + 1
        if i == 250:
            helpers.bulk(es, actions)
            actions = []
            i = 0

    if i > 0:
        helpers.bulk(es, actions)

csvfile.close()
