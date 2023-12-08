from py.TypeTitre import TitreCount as Q1
from py.NbFilmAnnees import TitreCount as Q2
from py.Interval import RatingRangeCount as Q3
from py.FileHandler import setup_temp, get_basics_tsv, cleanup

def runner(mr_job):
    with mr_job.make_runner() as runner:
        runner.run()

if __name__ == '__main__':
    print("########## Partie 1 - Setup ##########")
    print("Aquiring basics.tsv")
    get_basics_tsv()
    print("Preparing Q3")
    setup_temp()

    print("########## Partie 1 - Question 1 ##########")
    runner(Q1(["./tsv/basics.tsv","-o","outputs/Q1/"]))

    print("########## Partie 1 - Question 2 ##########")
    runner(Q2(["./tsv/basics.tsv","-o","outputs/Q2/"]))

    print("########## Partie 1 - Question 3 ##########")
    runner(Q3(["./outputs/temp.txt","-o","outputs/Q3/"]))

    print("########## Partie 1 - Clean Up ##########")
    cleanup()