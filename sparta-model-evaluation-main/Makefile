DATASET = enron
INTERVAL = 60
OVERHEAD_FACTOR = 100

DATA_DIR = data/$(DATASET)
RESULT_DIR = results/$(DATASET)
FIG_DIR = figs/$(DATASET)
TEST_DIR = tests

.PRECIOUS: $(DATA_DIR)/clean.csv

.PHONY: test data results figs

results: $(DATA_DIR)/clean.csv $(DATA_DIR)/users.csv \
	$(RESULT_DIR)/broadcast-$(INTERVAL).json \
	$(RESULT_DIR)/k-in-$(INTERVAL)-avg-$(OVERHEAD_FACTOR).json \
	$(RESULT_DIR)/k-in-$(INTERVAL)-user_avg-$(OVERHEAD_FACTOR).json \
	$(RESULT_DIR)/k-out-$(INTERVAL)-avg-$(OVERHEAD_FACTOR).json \
	$(RESULT_DIR)/k-out-$(INTERVAL)-user_avg-$(OVERHEAD_FACTOR).json \

$(RESULT_DIR)/broadcast-$(INTERVAL).json:
	python3 experiment.py $(DATA_DIR) $(RESULT_DIR) $(INTERVAL) broadcast

$(RESULT_DIR)/k-in-$(INTERVAL)-avg-$(OVERHEAD_FACTOR).json:
	python3 experiment.py $(DATA_DIR) $(RESULT_DIR) $(INTERVAL) system k-in --avg $(OVERHEAD_FACTOR)

$(RESULT_DIR)/k-in-$(INTERVAL)-user_avg-$(OVERHEAD_FACTOR).json:
	python3 experiment.py $(DATA_DIR) $(RESULT_DIR) $(INTERVAL) system k-in --user-avg $(OVERHEAD_FACTOR)

$(RESULT_DIR)/k-out-$(INTERVAL)-avg-$(OVERHEAD_FACTOR).json:
	python3 experiment.py $(DATA_DIR) $(RESULT_DIR) $(INTERVAL) system k-out --avg $(OVERHEAD_FACTOR)

$(RESULT_DIR)/k-out-$(INTERVAL)-user_avg-$(OVERHEAD_FACTOR).json:
	python3 experiment.py $(DATA_DIR) $(RESULT_DIR) $(INTERVAL) system k-out --user-avg $(OVERHEAD_FACTOR)

figs: results \
	$(FIG_DIR)/avg-i$(INTERVAL)-f$(OVERHEAD_FACTOR).txt \
	$(FIG_DIR)/latency-i$(INTERVAL)-f$(OVERHEAD_FACTOR).pdf \
	$(FIG_DIR)/ulatency-i$(INTERVAL)-f$(OVERHEAD_FACTOR).pdf \
	$(FIG_DIR)/ubandwidth-i$(INTERVAL)-f$(OVERHEAD_FACTOR).pdf \
	# $(FIG_DIR)/bandwidth-i$(INTERVAL)-f$(OVERHEAD_FACTOR).pdf \

$(FIG_DIR)/latency-i$(INTERVAL)-f$(OVERHEAD_FACTOR).pdf:
	python3 figs.py $(RESULT_DIR) $(FIG_DIR) $(DATASET) latency $(INTERVAL) $(OVERHEAD_FACTOR)

# $(FIG_DIR)/bandwidth-i$(INTERVAL)-f$(OVERHEAD_FACTOR).pdf:
# 	python3 figs.py $(RESULT_DIR) $(FIG_DIR) $(DATASET) bandwidth $(INTERVAL) $(OVERHEAD_FACTOR)

$(FIG_DIR)/ulatency-i$(INTERVAL)-f$(OVERHEAD_FACTOR).pdf:
	python3 figs.py $(RESULT_DIR) $(FIG_DIR) $(DATASET) ulatency $(INTERVAL) $(OVERHEAD_FACTOR)

$(FIG_DIR)/ubandwidth-i$(INTERVAL)-f$(OVERHEAD_FACTOR).pdf:
	python3 figs.py $(RESULT_DIR) $(FIG_DIR) $(DATASET) ubandwidth $(INTERVAL) $(OVERHEAD_FACTOR)

$(FIG_DIR)/avg-i$(INTERVAL)-f$(OVERHEAD_FACTOR).txt:
	python3 figs.py $(RESULT_DIR) $(FIG_DIR) $(DATASET) avg $(INTERVAL) $(OVERHEAD_FACTOR)

# Cleans the dataset.
# $(DATA_DIR)/clean.csv: data_clean/clean.py
# 					   $(DATA_DIR)/raw.csv
# 	echo "png $*"

# # Downloads the specified dataset.
# $(DATA_DIR)/raw.csv:
# 	@ echo "Downloading $(DATASET)"

test:
	python3 -m unittest discover -s $(TEST_DIR)
