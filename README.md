# Literature Analysis Bulk Insertion

Project to insert bulk data into new elasticsearch index


There are 2 scripts in this git repo. 
1. data_preparation.py: It will query existing index "beckett_jstor_ngrams_part" and process each document. 200 processed documents will be stored in json file in Data folder.
2. bulk_insertion.py: It will create new index with name "beckett_jstor_ngrams_termvectors" using mapping.json mapping. After creation on this new index all the json files from the Data folder will be processed and deleted after processing.

### NOTE: 
If we have linux server then we shouls use nohup command.

##### Commands to run scripts
  ######  pip install requirement.txt
  ######  python data_preparation.py 0
  ######  python bulk_insertion.py
