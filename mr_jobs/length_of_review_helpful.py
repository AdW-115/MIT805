'''
map reduce script for outputting the average number of helpful votes a review has based on the length of the review
'''
import re
import json
from mrjob.job import MRJob
from mrjob.step import MRStep

class AvgHelpfulVotes(MRJob):
    
    def mapper(self, key, value):

        review_data = json.loads(value)
        review_text = review_data['text']

        tokens = re.findall(r"\b\w+\b", review_text.lower())
        length_of_review = len(tokens)

        helpful_votes = review_data['helpful_vote']

        yield length_of_review, helpful_votes

    def combiner(self, key, values):

        count = 0
        summ = 0
        for value in values:
            count += 1
            summ += value

        yield key, summ/count

    def reducer(self, key, values):
        
        count = 0
        summ = 0
        for value in values:
            count += 1
            summ += value

        yield key, summ/count

    def steps(self):
        return [
            MRStep(
                mapper = self.mapper,
                combiner = self.combiner,
                reducer = self.reducer
            )
        ]
    
if __name__ == '__main__':
    AvgHelpfulVotes.run()
