#!/bin/sh
echo 'Clearing data folder...'
if [[ ! -d "data" ]]
then
  mkdir ./data
fi
rm -r data/*
echo 'Building tax lot dataset...'
python ./scripts/lots.py
echo 'Building census tract dataset...'
python ./scripts/tracts.py
echo 'Building complaint and problem datasets...'
python ./scripts/problems-complaints.py
echo 'Building violation dataset...'
python ./scripts/violations.py
echo 'Joining lot level dataset...'
python ./scripts/join-lots.py
echo 'Joining tract level dataset...'
python ./scripts/join-tracts.py
echo 'Joining complaint level dataset...'
python ./scripts/join-complaints.py
