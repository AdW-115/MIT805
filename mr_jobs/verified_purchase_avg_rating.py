'''
map reduce script outputting the average rating of products that have verified purchases vs non-verified purchases.
'''
import json
from mrjob.job import MRJob
from mrjob.step import MRStep

class AvgRating(MRJob):
    
    def mapper(self, key, value):

        review_data = json.loads(value)
        verified = review_data['verified_purchase']

        rating = review_data['rating']

        yield verified, rating

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
    AvgRating.run()
