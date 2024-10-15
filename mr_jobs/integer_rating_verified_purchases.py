'''
map reduce script for outputting the percentage of verified purchases by integer rating.
'''
import json
from mrjob.job import MRJob
from mrjob.step import MRStep

class AvgRating(MRJob):
    
    def mapper(self, key, value):

        verified_purchase = 0
        review_data = json.loads(value)
        interger_rating = round(review_data['rating'])

        if review_data['verified_purchase'] == True:
            verified_purchase = 1

        yield interger_rating, verified_purchase

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
