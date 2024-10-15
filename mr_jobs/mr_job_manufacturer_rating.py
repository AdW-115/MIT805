'''
map reduce script for outputting the average rating of the product manufacturer
'''
import json
from mrjob.job import MRJob
from mrjob.step import MRStep

class AvgRating(MRJob):
    
    def mapper(self, key, value):

        meta_data = json.loads(value)
        try:
            manufacturer = meta_data['details']['Manufacturer']
        except:
            manufacturer = 'unknown'

        avg_rating = meta_data['average_rating']

        yield manufacturer, avg_rating

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
