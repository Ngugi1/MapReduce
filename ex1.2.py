# Task 1 - find all entries of type short and movie
# Task 2 - Find top 50 most used keywords in primary titles
# Task 1 - find all entries of type short and movie
# Task 2 - Find top 50 most used keywords in primary titles
from mrjob.job import MRJob, MRStep
import re
import numpy as np
import nltk
from nltk.corpus import stopwords
import time
# Tab regular expression
TAB_REGEX = re.compile(r"[^\t]+")
ALPHA_NUM = re.compile(r"^[a-zA-Z0-9]+$")
movie_tag = "movie"
en_fn_stopwords = set(stopwords.words('english') + stopwords.words('french') + stopwords.words('spanish'))
# Define a job that inherits from MRJob
class MRTopWords(MRJob):

    def mapper_get_words(self, _, movie):
       # Split the record into constituent parts
        words = TAB_REGEX.findall(movie)
        primary_title = words[2]
        movie_type = words[1]
        if movie_type == movie_tag:
            # Yeild the word and 1
            for word in primary_title.split():
                # Drop stop words here to reduce amount of data flowing to reducer
                if not (word.lower() in en_fn_stopwords) and bool(ALPHA_NUM.match(word)):
                # Emit with keyword and 1
                    yield word.lower(), 1

    def combiner_count_words(self, word, counts):
        # sum the words we've seen so far
        yield (word, sum(counts))

    def reducer_count_words(self, word, counts):
        # send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is so we can easily use Python's max() function.
        yield None, (word, sum(counts))

    # discard the key; it is just None
    def reducer_find_top_words(self, _, word_count_pairs):
        # All  (word,count) will come to this reduce
        # A buffer keeps a list of top words seen so far
        buffer = [(None, -1) for i in range(15)]
        # values is a generator - ask it for the next value
        item = next(word_count_pairs, None)
        # While our generator still has some values, 
        # replace the word in the buffer that has less frequency
        while item != None:
            # Title and count of current item
            _ , count = item
            for i in range(15):
                # Buffered count
                _ , buffered_count = buffer[i]
                # If the current item has less count than current item
                if buffered_count < count:
                    buffer[i] = item
                    # Word with lowest count is always at the begining of the list
                    buffer.sort(key=lambda x: x[1], reverse=True)
                    break
            item = next(word_count_pairs, None)
        # Yield all the words in the buffer
        for (word, count) in buffer:
            yield word, count

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                    combiner=self.combiner_count_words,
                    reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_top_words)
        ]


if __name__ == '__main__':
    MRTopWords.run()