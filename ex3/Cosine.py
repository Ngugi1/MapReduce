# To run the program use python Cosine.py arxivData.json #

from mrjob.job import MRJob
from mrjob.step import MRStep
from nltk.corpus import stopwords
import json
import numpy as np

# Summary to match will be provided in search_text.txt file by user
summary_to_search = open("search_text.txt", "r")

# stop words set to remove generic words for better accuracy while calculating jaccard index and distance
stop_words = stopwords.words('english')

# Creating list from search text, which will be used to compare with details of paper being matched         
search_content = [word.lower() for word in summary_to_search.read().split() if not word.lower() in stop_words]


class CosineofSummary(MRJob):

    def steps(self):
        
        return [
            MRStep(mapper_raw=self.mapper_id_summary,   # mapper to preprocess json file
                   reducer=self.reducer_id_summary)     # reducer to obtain id, summary mapping
            ,
            MRStep(
                mapper=self.mapper_cosine,               # mapper for calculation of cosine distance
                reducer=self.reducer_cosinematch         # reducer to provide highest cosine for matching paper
            )
        ]
        
    # # yields id of paper and tokenized set of words for each paper    
    def mapper_id_summary(self, path, value):

        # Loading json file provided in argument (ex: arxiv.json)
        dataset_paper = json.load(open(path, "r"))

        # for each paper preprocess content to form id, words pair
        for paper in dataset_paper:
            id = paper.get('id')
            summary = paper.get('summary').split()
            
            for word in summary:
                if word not in stop_words:
                    yield id, word.lower()
    
    # reducer to process mapper output to form (id, list of words) set for each paper
    def reducer_id_summary(self, id, word):
        yield id, list(word)

    # yield cosine distance and id for paper being matched
    def mapper_cosine(self, id, summary_content):

        # Creating bag of words model for whole content (Paper summary + Search content))

        all_words = summary_content + search_content
        
        # dictionary to store word,frequency of whole content
        word_dist = {}
        
        # computing frequnecy of words in whole content
        for word in all_words:
            word.lower()
            if word not in word_dist.keys():  
                word_dist[word] = 1
            else:  
                word_dist[word] += 1
        

        # Bag of Words vectorization for provided content against whole content
        def vectorize(content, vocabulary):
            
            # to align size of vectors before cosine is applied which expects same sized vectors
            vector_summary = np.zeros(len(vocabulary))
            
            # creating vectorized model
            for i, keyWord in enumerate(vocabulary.keys()):
                for word in content:
                    if keyWord == word:
                        vector_summary[i] += 1
            return vector_summary

       
        # creating word distribution for summary and search content against whole content
        vector_summary = vectorize(summary_content, word_dist)
        vector_search = vectorize(search_content, word_dist)
        
        # Cosine Similarity calculation for vectors
        # Cos(Theta) = (A.B)/ ||A||||B|| where . is dot product and || is magnitude
        
        dot_vector = np.dot (vector_summary, vector_search)
        magnitude_vector_summary = np.linalg.norm(vector_summary)
        magnitude_vector_search = np.linalg.norm(vector_search)
        cosine = dot_vector / (magnitude_vector_summary * magnitude_vector_search)
        
        yield None, (cosine, id)
    
    # reducer to provide max cosined with related paper id
    def reducer_cosinematch(self, _, cosine_id):
        yield 'Cosine value and most matching paper: ', max(cosine_id)


if __name__ == '__main__':
    CosineofSummary.run()










