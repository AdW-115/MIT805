'''
map reduce script outputting the popularity of product manufacturer based on the number of reviews.
'''
import json
from mrjob.job import MRJob
from mrjob.step import MRStep

class SumOfRating(MRJob):
    
    def mapper(self, key, value):

        meta_data = json.loads(value)
        try:
            manufacturer = meta_data['details']['Manufacturer']
        except:
            manufacturer = 'unknown'

        num_rating = meta_data['rating_number']

        yield manufacturer, num_rating

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
