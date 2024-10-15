'''
map reduce script for outputting the average number of helpful votes a review has based on whether images were included or not.
'''
import json
from mrjob.job import MRJob
from mrjob.step import MRStep

class AvgHelpfulVotes(MRJob):
    
    def mapper(self, key, value):

        image_binary = 0
        review_data = json.loads(value)
        images = review_data['images']
        if len(images) > 0:
            image_binary = 1

        helpful_votes = review_data['helpful_vote']

        yield image_binary, helpful_votes

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
