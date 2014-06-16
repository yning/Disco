#!/usr/bin/env python
from disco.core import Job, result_iterator
from mrtweets import map_tweet_reader
import worker

class FilterTweets(Job):
    params = None
    map_reader = staticmethod(map_tweet_reader)

    @staticmethod
    def map(entry, params):
        import simplejson as json
        from datetime import datetime
        from utils import tweet_text_tokenizer

        format_hashtag = lambda x: "".join(['#', x])
        format_mention = lambda x: "".join(['@', x])
        binify = lambda x: x.replace(hour=x.hour // 12 * 12, minute=0, second=0)

        k, v = entry
        #day = binify(datetime.fromtimestamp(k)).strftime("%Y-%m-%dT%H.json")
        day = datetime.fromtimestamp(k).strftime("%Y-%m-%d.csv")

        for t in json.loads(v):
            if 'twitter' in t and 'retweet' not in t['twitter'] and 'text' in t['twitter']:
                text = t['twitter']['text'].strip()
    
                urls, mentions, hashtags, text_tokens = tweet_text_tokenizer(text)
                if not hashtags and len(text_tokens) <= 1:
                    continue
                #tokens = set(urls + [format_mention(m.lower()) for m in mentions] +
                #             [format_hashtag(h.lower()) for h in hashtags] + text_tokens)
                tokens = set([format_hashtag(h.lower()) for h in hashtags] + text_tokens)
                #t2 = {'tokens': list(tokens), 'timestamp': k,
                 #     'tweet_id': str(t['twitter']['id'])}
                outputstr = ",".join([str(t['twitter']['id']),str(k)] + list(tokens))
                #yield day, (k, json.dumps(t2, encoding='utf8', ensure_ascii=False).encode('utf8'))
                yield day, (k, json.dumps(outputstr, encoding='utf8', ensure_ascii=False).encode('utf8'))

    @staticmethod
    def reduce(rows_iter, params):
        from disco.util import kvgroup
        for k, vs in kvgroup(sorted(rows_iter, key=lambda x:x[0])):
            yield k, "\n".join([v for i, v in vs])


if __name__ == '__main__':
    import os
    import sys
    import json
    import argparse
    from  yn_filter_tweets_mr import FilterTweets
    from disco.ddfs import DDFS
    from datetime import timedelta
    import getpass
    from utils import parse_time, split_list
    uname = getpass.getuser()
    ap = argparse.ArgumentParser("")
    ap.add_argument('--start', metavar='START', type=str, required=True,
                    help='date (YYYY-MM-DD)')
    ap.add_argument('--stop', metavar='STOP', type=str, required=True, 
                    help='date (YYYY-MM-DD) till when the scoring period ends')
    ap.add_argument('--tag', metavar='TAG', type=str, default='twitter',
                    help='twitter tag to use')
    args = ap.parse_args()

    start = parse_time(args.start)
    stop = parse_time(args.stop)

    DUFUS = DDFS()
    total_days = []
    cur = start
    while cur <= stop:
        total_days.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)

    parts = 10
    if len(total_days) > parts:
        parts = len(total_days) // parts
    else:
        parts = 1

    split_days = split_list(total_days, wanted_parts=parts)    

    for setid, days in enumerate(split_days):
        days = sorted(days)
        print "#days", len(days), days[0], days[-1]
        inputs = []
        for d in days:
           inputs += DUFUS.list("%s:%s" % (args.tag, d))

        input_filename = [("tag://%s" % i) for i in inputs]

        print "days", len(days), "files", len(input_filename)
        job = FilterTweets(worker=worker.Worker()).run(
            input=input_filename,
            partitions = len(days),
            name="%s:FilterTweets" % (uname),
        )

        for day, tweets in result_iterator(job.wait(show=False)):
            with open(day, "w") as out:
                out.write(tweets)
