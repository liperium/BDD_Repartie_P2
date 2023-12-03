#!/bin/bash

cd tsv
curl -o basics.gz https://datasets.imdbws.com/title.basics.tsv.gz
cd ..

echo ''
echo " PLEASE EXTRACT data.tsv THEN COMMENT LINE 4"
echo ''

echo ''
echo "########## Partie 1 - Question 1 ##########"
echo ''
python ./py/TypeTitre.py ./tsv/data.tsv > outputQ1.txt

echo ''
echo "########## Partie 1 - Question 2 ##########"
echo ''
python ./py/NbFilmAnnees.py ./tsv/data.tsv > outputQ2.txt

echo ''
echo "########## Partie 1 - Question 3 ##########"
echo ''
python ./py/Interval.py ./tsv/ratings.tsv > outputQ3.txt