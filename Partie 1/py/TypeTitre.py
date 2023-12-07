from mrjob.job import MRJob
from mrjob.step import MRStep

class TitreCount(MRJob):
    def mapper(self, _, line):
        # On divise les champs de l'entrée.
        fields = line.split("\t")
        
        # Extraction des noms des titres et mise du champ à 1.
        title_type = fields[1]
        
        yield(title_type, 1)
            
    def reducer(self, title_type, counts):
        total_count = sum(counts)
        
        # Agrège les résultats pour avoir nombre total de chaque film.
        yield None, (total_count, title_type)

    def reducer_sort(self, _, count_title_pairs):
        
        # Trie des valeurs par ordre décroissant.
        sorted_pairs = sorted(count_title_pairs, reverse=True)
        
        # Résultat final.
        for count, title_type in sorted_pairs:
            yield title_type, count

    def steps(self):
        return [
            #Découpage en 2 phases : traitement puis tri.
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.reducer_sort)
        ]

if __name__ == '__main__':
    TitreCount.run()
