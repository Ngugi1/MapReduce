# To run the program use python Jaccard.py arxivData.json #

from mrjob.job import MRJob
from mrjob.step import MRStep
from nltk.corpus import stopwords
import json

# Summary to match will be provided in search_text.txt file by user
summary_to_search = open("search_text.txt", "r")

# stop words set to remove generic words for better accuracy while calculating jaccard index and distance
stop_words = stopwords.words('english')

# Creating list from search text, which will be used to compare with details of paper being matched         
search_content = [word.lower() for word in summary_to_search.read().split() if not word.lower() in stop_words]


class JacardDistanceofSummary(MRJob):

    def steps(self):
        
        return [
            MRStep(mapper_raw=self.mapper_id_summary,   # mapper to preprocess json file
                   reducer=self.reducer_id_summary)     # reducer to obtain id, summary mapping
            ,
            MRStep(
                mapper=self.mapper_jaccard,             # mapper for calculation of jaccard index and distance
                reducer=self.reducer_minjaccard         # reducer to obtain minimum jacard distance matching paper
            )
        ]



    # yields id of paper and tokenized set of words for each paper
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

    # yield jacard distance and id for paper being matched
    def mapper_jaccard(self, id, summary_content):
        
        # computing jaccard distance
        def compute_jacard_distance(to_search, match_summary):
            
            # https://www.statisticshowto.datasciencecentral.com/jaccard-index
            # Formula being used for calculation of Jaccard distance for set A and Set B
            # Jaccard distane = 1 - Jaccard index
            # where Jaccard index = len(intersection of set A and B) /
            #                       (len(set A) + len(set B) - len(intersection of set A and B))
            
            set_intersection = set(to_search).intersection(set(match_summary))
            jaccard_index = len(set_intersection) / (len(to_search) + len(match_summary) - len(set_intersection))
            computed_jaccard_distance = 1 - jaccard_index
            
            return computed_jaccard_distance
        
        yield None, (compute_jacard_distance(summary_content, search_content), id)
    
    # reducer to provide minimum jaccard index related paper id
    def reducer_minjaccard(self, _, jaccardsummary_id):
        yield 'Minimum Jaccard distance and most matching paper ', min(jaccardsummary_id)


if __name__ == '__main__':
    JacardDistanceofSummary.run()