from mrjob.job import MRJob
from mrjob.step import MRStep

class TitreCount(MRJob):
    def mapper(self, _, line):
        # Split the line into fields
        fields = line.split("\t")
        
        # Extract the title type
        title_type = fields[1]
        
        # Emit a count of 1 for the title type
        yield(title_type, 1)
            
    def reducer(self, title_type, counts):
        # Sum the counts for each title type
        total_count = sum(counts)
        
        # Emit the total count as the key for sorting
        yield None, (total_count, title_type)

    def reducer_sort(self, _, count_title_pairs):
        # Sort the results by total count in descending order
        sorted_pairs = sorted(count_title_pairs, reverse=True)
        
        # Emit the sorted results
        for count, title_type in sorted_pairs:
            yield title_type, count

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.reducer_sort)
        ]

if __name__ == '__main__':
    TitreCount.run()
