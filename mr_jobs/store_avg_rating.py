'''
map reduce script outputting the average rating of seller stores.
'''
import json
from mrjob.job import MRJob
from mrjob.step import MRStep

class AvgRating(MRJob):
    
    def mapper(self, key, value):

        meta_data = json.loads(value)
        store_name = meta_data['store']

        avg_rating = meta_data['average_rating']

        yield store_name, avg_rating

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
