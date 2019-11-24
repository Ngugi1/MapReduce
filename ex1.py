# Task 1 - find all entries of type short and movie
# Task 2 - Find top 50 most used keywords in primary titles
from mrjob.job import MRJob
import re
import numpy as np
import nltk
from nltk.corpus import stopwords
import time
# Tab regular expression
TAB_REGEX = re.compile("[^\t]+")
movie_tag = "movie"
short_tag = "short"
en_fn_stopwords = set(stopwords.words('english') + stopwords.words('french') + stopwords.words('spanish'))
# Define a job that inherits from MRJob
class Top50Keywords(MRJob):
    def mapper(self, _, movie):
        # Split the record into constituent parts
        movie = TAB_REGEX.findall(movie)
        primary_title = movie[2]
        movie_type = movie[1]
        if movie_type == short_tag or movie_type == movie_tag:
            # Yeild the word and 1
            for word in primary_title.split():
                # Drop stop words here to reduce amount of data flowing to reducer
                if not (word in en_fn_stopwords):
                # Emit with keyword and 1
                    yield word.lower(), 1

    # Combine to count each occourence of the word
    def combiner(self,key, values):
        # Emit all words with key none so that we send it to
        # the same reducer
        yield None, (key, sum(values))
    # Here, see which words are most popular
    def reducer(self, _, values):
        # All  (word,count) will come to this reduce
        # A buffer keeps a list of top words seen so far
        buffer = [('placeholder', 0) for i in range(30)]
        # values is a generator - ask it for the next value
        item = next(values, None)
        # While our generator still has some values, accumulate them in the buffer
        while item != None:
            # Title and count of current item
            title, count = item
            for i in range(30):
                # Buffered count
                _ , buffered_count = buffer[i]
                # If the current item has less count that current item
                # And the word is not a stop word, make itto be in top 30
                if buffered_count < count and not (title in en_fn_stopwords):
                    buffer[i] = item
                    break
            item = next(values, None)
        # Filter the buffer to remove stopwords - i.e. articles e.t.c
        buffer = [(word,count) for word,count in buffer if not (word in en_fn_stopwords)]
        # Yield all the words in the buffer
        for (word, count) in buffer:
            yield word, count

# Test the job
if __name__ == '__main__':
    start = time.time()
    Top50Keywords.run()
    print((time.time() - start) / 60)