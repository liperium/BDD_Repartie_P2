from mrjob.job import MRJob

class TitreCount(MRJob):
    def mapper(self, _, line):
        # Split the line into fields
        fields = line.split("\t")
        
        # Extraction de titres et des années 
        title_type = fields[1]
        start_year = fields[5]
        
        if title_type in ["video", "movie"]:
            yield(start_year, 1)
            
    def reducer(self, start_year, counts):
        # Somme des occurences s
        yield(start_year, sum(counts))
        
if __name__ == '__main__':
    TitreCount.run()
