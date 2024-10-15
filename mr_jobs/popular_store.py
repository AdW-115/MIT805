'''
map reduce script outputting the popularity seller stores based on the number of reviews.
'''
import json
from mrjob.job import MRJob
from mrjob.step import MRStep

class SumOfRating(MRJob):
    
    def mapper(self, key, value):

        meta_data = json.loads(value)
        store_name = meta_data['store']

        num_rating = meta_data['rating_number']

        yield store_name, num_rating

    def combiner(self, key, values):

        yield key, sum(values)

    def reducer(self, key, values):
        
        yield key, sum(values)

    def steps(self):
        return [
            MRStep(
                mapper = self.mapper,
                combiner = self.combiner,
                reducer = self.reducer
            )
        ]
    
if __name__ == '__main__':
    SumOfRating.run()
