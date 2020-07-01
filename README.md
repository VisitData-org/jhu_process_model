# jhu_model_process
Code for generating summary aggregates from the simulation runs of JHU's [https://github.com/HopkinsIDD/COVIDScenarioPipeline Covid Scenario Pipeline].

To use:
1) export AWS_ACCESS_KEY_ID=<your id>; export AWS_SECRET_ACCESS_KEY=<your key>
2) Download the simulation output files 
3) ./process_jhu_data_to_s3.R -i <inputdir> -o <outputdir> -d=YYYYMMDD --add_counties

This reads the simulation files from inputdir/YYYYMMDD and writes summary csv files to outputdir/YYYYMMDD and uploads the files to amazon S3://jhumodelaggregates/{<date>,latest}, one sim summary per modeled scenario
