'''
map reduce script outputting the popularity of main product categories based on the number of reviews.
'''
import json
from mrjob.job import MRJob
from mrjob.step import MRStep

class SumOfRating(MRJob):
    
    def mapper(self, key, value):
        meta_data = json.loads(value)
        main_category = meta_data['main_category']
        number_of_ratings = meta_data['rating_number']

        yield main_category, number_of_ratings

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
