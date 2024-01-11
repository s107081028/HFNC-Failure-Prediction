## Prediction of High Flow Oxygen Treatment Failure in Patients with Hypoxic Respiratory Failure
### Data
 - HFNC_Collection.sql: Collect data from MIMIC-IV v2.2
 - HFNC_Preprocessing: Data preprocessing on collected data
 - HFC_Training: Model training on preprocessed data
 - HFNC_latest.csv: Raw features
 - HFNC_median.csv: Preprocessed features

### Data Collection
Run HFNC_Collection.sql on MIMIC-IV dataset, and receive "HFNC_latest.csv" as raw data.

### Preprocessing
Make sure previous csv is named as "HFNC_latest.csv", and run HFNC_Preprocessing.ipynb.
"HFNC_median.csv" will be produced.

### Model Training
Make sure "HFNC_latest.csv" and "HFNC_median.csv" both exist.
Run HFNC_Training.ipynb, receiving the results and plots. 
