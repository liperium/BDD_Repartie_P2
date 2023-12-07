from mrjob.job import MRJob
from mrjob.step import MRStep

class TitreCount(MRJob):
    def mapper(self, _, line):
        # Split the line into fields
        fields = line.split("\t")

        # Extraction de titres et des années 
        title_type = fields[1]
        start_year = fields[5]

        if title_type in ["video", "movie"]:
            if start_year != "\\N":  # Vérifie si start_year n'est pas manquant
                yield start_year, 1

    def reducer(self, start_year, counts):
        # Somme des occurrences
        yield start_year, sum(counts)

    def mapper_sort(self, start_year, count):
        # Vérifie si start_year n'est pas manquant avant de le convertir
        if start_year != "\\N":
            yield None, (int(start_year), count)

    def reducer_sort(self, _, year_count_pairs):
        # Trie les résultats par année
        sorted_pairs = sorted(year_count_pairs)

        # Émet les résultats triés
        for start_year, count in sorted_pairs:
            yield str(start_year), count

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(mapper=self.mapper_sort,
                   reducer=self.reducer_sort)
        ]

if __name__ == '__main__':
    TitreCount.run()
