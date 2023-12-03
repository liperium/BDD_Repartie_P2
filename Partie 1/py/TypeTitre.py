from mrjob.job import MRJob

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
        yield(title_type, sum(counts))
        
if __name__ == '__main__':
    TitreCount.run()
