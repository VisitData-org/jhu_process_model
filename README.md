# jhu_model_process
Code for processing JHU model outputs.

Required:
- relevant AWS account access (ask if you don't know what this means)
- appropriate Python & env

To use:
1) export AWS_ACCESS_KEY_ID=<your id>; export AWS_SECRET_ACCESS_KEY=<your key>
2) /bin/sh scripts/setup.sh 
3) python scripts/process_jhu_data_to_s3.py

Writes output CSVs to S3://jhumodelaggregates/{<date>,latest}, one sim summary per modeled scenario in:
[ 'No_Intervention', 'Statewide KC 1918', 'Statewide_Lockdown_8_Weeks', 'UK-{Fixed,Fatigue}-8w-Fol{Mild,Pulse} ]
