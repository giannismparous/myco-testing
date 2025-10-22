#! /bin/bash
DATASET=enron
INTERVAL=60

python3 experiment.py data/$DATASET results/$DATASET $INTERVAL k-out --varies 10
python3 experiment.py data/$DATASET results/$DATASET $INTERVAL k-out --fixed 100
python3 experiment.py data/$DATASET results/$DATASET $INTERVAL k-in --fixed 100