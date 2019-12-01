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
short_tag = "short"
en_fn_stopwords = set(stopwords.words('english') + stopwords.words('french') + stopwords.words('spanish'))
# Define a job that inherits from MRJob
class MRTop50Word(MRJob):

    def mapper_get_words(self, _, movie):
       # Split the record into constituent parts
        words = TAB_REGEX.findall(movie)
        primary_title = words[2]
        movie_type = words[1]
        if movie_type == short_tag or movie_type == movie_tag:
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
        buffer = [(None, -1) for i in range(50)]
        # values is a generator - ask it for the next value
        item = next(word_count_pairs, None)
        # While our generator still has some values, accumulate them in the buffer
        while item != None:
            # Title and count of current item
            _ , count = item
            _ , buffered_count = buffer[0]
            # If the current item has less count that current item
            if buffered_count < count:
                buffer[0] = item
                # Word with lowest count is always at the begining of the list
                buffer.sort(key=lambda x: x[1])
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
    start = time.time()
    MRTop50Word.run()
    print((time.time() - start) / 60)
