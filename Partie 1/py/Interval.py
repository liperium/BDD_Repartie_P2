from mrjob.job import MRJob
import pandas

intervals = [[0.0, 1.0], [1.0, 2.0], [2.0, 3.0], [3.0, 4.0], [4.0, 5.0], [5.0, 6.0], [6.0, 7.0], [7.0, 8.0], [8.0, 9.0], [9.0,10.0]]
class RatingRangeCount(MRJob):

    def mapper(self, _, line):
        # Split the line into fields
        fields = line.split("\t")

        if fields[1] == 'averageRating':
            return
        # Extraction de titres et des annÃ©es 
        rating = float(fields[1])

        for i in range(0,len(intervals)):
            if rating <= intervals[i][1] and rating > intervals[i][0]:
                yield(intervals[i], 1)
            elif rating == 0.0:
                yield (intervals[0], 1)
            
    def reducer(self, intervals, counts):
        # Somme des occurences s
        yield(intervals, sum(counts))
        
if __name__ == '__main__':
    RatingRangeCount.run()