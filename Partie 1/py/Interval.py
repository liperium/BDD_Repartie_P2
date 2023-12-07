from mrjob.job import MRJob
from mrjob.step import MRStep

# Définition des intervalles
intervals = [[0.0, 1.0], [1.0, 2.0], [2.0, 3.0], [3.0, 4.0], [4.0, 5.0], [5.0, 6.0], [6.0, 7.0], [7.0, 8.0], [8.0, 9.0], [9.0, 10.0]]

class RatingRangeCount(MRJob):

    def mapper(self, _, line):
        # On divise les champs de l'entrée.
        fields = line.split("\t")

        if fields[1] == 'averageRating':
            return
        # Extraction de titres et des années 
        rating = float(fields[1])

        # Ajout d'un indice représentant chaque intervalle
        for i in range(0, len(intervals)):
            if rating <= intervals[i][1] and rating > intervals[i][0]:
                yield (i, 1)  # Utilise l'indice i pour permettre le tri
            elif rating == 0.0:
                yield (0, 1)  # Utilise l'indice 0 pour le cas rating == 0.0

    def reducer(self, interval_index, counts):
        # Somme des occurrences
        yield (interval_index, sum(counts))

    def mapper_sort(self, interval_index, count):
        # Utilise l'indice pour trier
        yield None, (interval_index, count)

    def reducer_sort(self, _, index_count_pairs):
        # Trie les résultats par indice d'intervalle
        sorted_pairs = sorted(index_count_pairs)
        
        # Émet les résultats triés
        for interval_index, count in sorted_pairs:
            yield str(intervals[interval_index]), count

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(mapper=self.mapper_sort,
                   reducer=self.reducer_sort)
        ]

if __name__ == '__main__':
    RatingRangeCount.run()
