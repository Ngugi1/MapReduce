from mrjob.job import MRJob, MRStep
from mrjob.protocol import RawValueProtocol
import nltk
from nltk.corpus import stopwords
import re
import mrjob
import json
import csv
from random import randint
en_stopwords = set(stopwords.words('english'))
is_paper_selected = False
random_paper = ()
class MRLeastJacardDist(MRJob):
    def mapper(self, _, line):
        try:
            paper = line.split('§')
            summary = paper[1].split(" ")
            words = set([word.lower() for word in summary if word not in en_stopwords])
            common_words = len(words.intersection(random_paper[1]))
            r_p = [word.lower() for word in random_paper[1] if word not in en_stopwords]
            all_words = len(words.union(r_p))
            jacard_distance = 100 - (common_words / all_words * 100)
            # Don't compare  a paper with itself
            if random_paper[0] != paper[0]:
              yield None, (paper[1], jacard_distance)

        except:
            raise Exception('Execption {}'.format(line))

    def reducer (self,_,values):
        # For every paper ypu receive, check if it has a smaller value
        # And keep it
        item = next(values, None)
        initial_distance = item
        while item is not None:
            if item[1] < initial_distance[1]:
                initial_distance = item
            item = next(values, None)
        # Yield the paper with the least distance
        yield  initial_distance

if __name__ == '__main__':
    # Preprocess data before passing it to MRJob
    data_file = open('input.csv', mode='w')
    data_writer = csv.writer(data_file, delimiter='§')
    with open('arxivData.json') as json_file:
        papers = json.load(json_file)
        for paper in papers:
            # Remove all the newline and carriage returns
            # Also just save a small part of data that is useful for MR
            # From Summary, remove all punctuation and newlines
            summary = paper["summary"].replace('\n', ' ')
            summary = re.sub(r'[^\w\s]','',summary)
            # Randomly select a paper
            if not is_paper_selected:
                val = randint(0, 100)
                if len(summary) > val:
                    random_paper = (paper["title"].replace('\n', ' '), set(summary.split(" ")))
                    is_paper_selected = True
            data_writer.writerow([paper["title"].replace('\n', ' '), summary])

    data_file.close()
    json_file.close()
    MRLeastJacardDist.run()
