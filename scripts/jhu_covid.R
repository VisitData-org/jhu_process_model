#
# Functions for processing JHU simulation files 
# Ryan McCorvie
# Copyright 2020
#

shhh <- suppressPackageStartupMessages # It's a library, so shhh!

shhh(require( "arrow" ))
shhh(require( "lubridate" ))
shhh(require( "tidyverse" ))
shhh(require( "argparse"))
shhh(require( "aws.s3" ))

AWS_ACCESS_KEY_ID     = ""
AWS_SECRET_ACCESS_KEY = ""
AWS_DEFAULT_REGION    = "us-east-2"
S3_BUCKET_NAME        = "jhumodelaggregates"
MAXDATE               = ymd("21000101")

CA_FIPS_REGEX = "^06[0-9]{3}$"

OUTPUT_SUFFIXES = c( '_mean', '_median', '_q25', '_q75' )

DATA_OUTPUT_COLS = c( 'hosp_occup', 'hosp_admit', 'icu_occup','icu_admit','new_infect','new_deaths', "new_cases" )
JHU_REMAP_COLS = c('hosp_curr','incidH','icu_curr','incidICU','incidI','incidD', "incidC")
#DATA_OUTPUT_COLS = c( 'hosp_occup', 'hosp_admit', 'icu_occup','icu_admit','new_infect','new_deaths' )
#JHU_REMAP_COLS = c('hosp_curr','incidH','icu_curr','incidICU','incidI','incidD')
names( JHU_REMAP_COLS) = DATA_OUTPUT_COLS

PARTITIONING = c("location","scenario","death_rate","date", "lik_type", "is_final", "sim_id")

RUNDATE    = format(today(), "%Y%m%d")
IFR_PREFIX = 'low'

SCENARIOS = tribble(
  ~inpath, ~scenario,
  'nonpi-hospitalization/model_output/unifiedNPI/',                                 'No Intervention',
  'kclong-hospitalization/model_output/mid-west-coast-AZ-NV_SocialDistancingLong/', 'Statewide KC 1918',
  'wuhan-hospitalization/model_output/unifiedWuhan/',                               'Statewide Lockdown 8 weeks',
  'hospitalization/model_output/mid-west-coast-AZ-NV_UKFixed_Mild',                 'UK-Fixed-8w-FolMild',
  'hospitalization/model_output/mid-west-coast-AZ-NV_UKFatigue_Mild',               'UK-Fatigue-8w-FolMild',
  'hospitalization/model_output/mid-west-coast-AZ-NV_UKFixed_Pulse',                'UK-Fixed-8w-FolPulse',
  'hospitalization/model_output/mid-west-coast-AZ-NV_UKFatigue_Pulse',              'UK-Fatigue-8w-FolPulse',
  'hospitalization/model_output/mid-west-coast-AZ-NV_Lockdown_continued',           'Continued Lockdown',
  'hospitalization/model_output/mid-west-coast-AZ-NV_Lockdown_fastOpen',            'Fast-paced Reopening',
  'hospitalization/model_output/mid-west-coast-AZ-NV_Lockdown_moderateOpen',        'Moderate-paced Reopening',
  'hospitalization/model_output/mid-west-coast-AZ-NV_Lockdown_slowOpen',            'Slow-paced Reopening',
  'west-coast-AZ-NV_Lockdown_continued',                                            'Continued Lockdown' ,
  'west-coast-AZ-NV_Lockdown_fastOpen',                                             'Fast-paced Reopening',
  'west-coast-AZ-NV_Lockdown_moderateOpen',                                         'Moderate-paced Reopening',
  'west-coast-AZ-NV_Lockdown_slowOpen',                                             'Slow-paced Reopening',
  'hospitalization/model_output/California_Lockdown_continued',                     'Continued Lockdown',
  'hospitalization/model_output/California_Lockdown_fastOpen',                      'Fast-paced Reopening',
  'hospitalization/model_output/California_Lockdown_moderateOpen',                  'Moderate-paced Reopening',
  'hospitalization/model_output/California_Lockdown_slowOpen',                      'Slow-paced Reopening',
  'hospitalization/model_output/California_June_inference',                         'Inference',
)


SCENARIOS_ARROW = tribble(
  ~scenario,         ~newscenario,
  "June_inference",  "Inference",
  "inference",  "Inference",
)

#' Set the credentials to be able to access appropriate S3 bucket
#'
#' Only sets credentials if they are not already set, unless the override flag is 
#' set to true.  Returns TRUE if operation successful 
#' 

s3_set_credentials <- function( override = FALSE )
{
  keyid <- Sys.getenv("AWS_ACCESS_KEY_ID")
  if( keyid != "" && !override )
    return( TRUE )
  
  all( Sys.setenv(
    "AWS_ACCESS_KEY_ID"     = AWS_ACCESS_KEY_ID,
    "AWS_SECRET_ACCESS_KEY" = AWS_SECRET_ACCESS_KEY,
    "AWS_DEFAULT_REGION"    = AWS_DEFAULT_REGION 
  ))
}


#' Replace ~ in path with path to home dir
#'

expand_home_dir <- function( path )
{
  if( !str_detect(path, "^~"))
    return( path )
  
  wd<-getwd()
  setwd("~")
  homedir <- getwd()
  setwd(wd)
  str_replace( path, "^~", homedir)
}


#' Set the credentials to be able to access appropriate S3 bucket
#'
#' Only sets credentials if they are not already set, unless the override flag is 
#' set to true.  Returns TRUE if operation successful 
#' 

upload_to_s3 <- function( outputloc, rundate = RUNDATE, latest=FALSE )
{
  s3_set_credentials()
  
  files <- list.files( file.path( outputloc, rundate ) )
  files <- files[ str_detect( files, "\\.csv$" ) ]
  
  if( !latest )
    print( paste("INFO: uploading", length(files), "files to s3", S3_BUCKET_NAME, "for rundate", rundate ) )
  else
    print( paste("INFO: uploading", length(files), "files to s3", S3_BUCKET_NAME, "latest" ) )
  for( file in files )
  {
    fullpath <- file.path( outputloc, rundate, file )
    if( latest )
      objname = paste( "latest", file, sep= "/")
    else
      objname = paste( rundate, file, sep= "/")
    
    put_object( file= fullpath, object= objname, bucket=S3_BUCKET_NAME)
  }
}


#' Read and filter one parquet file from JHU simulation
#'
#' Read parquet file of simulation and filters by california FIPS, returns a tibble
#'

read_jhu_onefile <- function( file_name )
{
  input_df = read_parquet(file_name)
  
  # some scenarios (e.g. Statewide KC 1918) have counties outside of CA, so ensure only CA counties present...
  input_df <- input_df %>% filter( str_detect( geoid, CA_FIPS_REGEX ) ) 

  #input_df <- input_df %>% filter( time> ymd("20200401") & time <= ymd("20200410"))
  
  return( input_df )
}

read_jhu_simfiles <- function( inputloc, rundate= RUNDATE, scenarios = SCENARIOS, IFR = IFR_PREFIX )
{
  out = NULL
  for(scen_idx in 1:nrow(scenarios))
  {
    inpath    = scenarios$inpath[scen_idx]
    scenario  = scenarios$scenario[scen_idx]
    
    input_dir = file.path(inputloc, rundate, inpath)
    if( !dir.exists( input_dir ))
    {
      print( paste("INFO: Skipping",scenario ,"because input directory",input_dir ,"does not exist"))
      next
    }
    
    files <- list.files( input_dir )
    files <- files[ str_detect( files, paste0("^", IFR )) ]
    files <- files[ str_count( files, "[1-9][0-9]*") ==  1 ]
    
    print( paste("INFO: Scenario", scenario, "IFR_PREFIX", IFR, "found", length(files), "simulation files"))
    
    df_list <- vector( mode="list", length = length(files))
    for( idx in 1:length(files ))
    {
      file = files[idx]
      
      file_num <- as.numeric(str_extract( file, "[1-9][0-9]*"))
      df  <- read_jhu_onefile( file.path( input_dir, file ))
      df <- df %>% mutate( file_num = file_num, scenario = scenario )
      df_list[[idx]] <- df
      if(idx%%25 ==0 )
        print( paste( "INFO: Processing file", idx, "/", length(files), "( id =",file_num ,")"))
    }
    scen_df <- bind_rows( df_list )
    out <- bind_rows( out, scen_df )
  }
  if( is.null(out))
    stop("ERROR: no simluation files found at ", file.path(inputloc,rundate))
  
  out <- out %>% rename( !!JHU_REMAP_COLS )
  out
}

rename_scenario <- function( scenario )
{
  ifelse( 
    is.na( match( scenario, SCENARIOS_ARROW$scenario )),
    scenario,
    SCENARIOS_ARROW$newscenario[ match( scenario,SCENARIOS_ARROW$scenario)]
  )
}

#' Allow for scenario dependent IFR

ifr_match <- function( scenario )
{
  return( if_else( scenario=="Cases", "med", 
      if_else( scenario == "Deaths", "med",
               IFR_PREFIX
  )))
}

#' Read all JHU simulation files for all scenarios
#'
#' Returns a tibble containing to all simulation runs and all scenarios 
#' (for a given IFR assumption)
#'

read_jhu_simulation <- function( inputloc, rundate= RUNDATE )
{
  print(paste("INFO: Reading JHU model output from", inputloc,"for date", rundate))
  # read in the raw model output scenario data for the scenarios in the SCENARIOS global...
  if( ymd(rundate ) < ymd( "20200623"))
    return(read_jhu_simfiles( inputloc, rundate ))
  
  # open_dataset doesn't like ~, so expand it in path names
  inputloc <- expand_home_dir( inputloc )
  
  simdir <- file.path(inputloc,rundate, "hosp")
  
  sim_arrow <- arrow::open_dataset( simdir, partitioning=PARTITIONING ) %>% collect()
  
  sim_arrow <- sim_arrow %>% 
    filter( time <= MAXDATE ) %>% 
    mutate( time= as_date( time, tz="UTC"), file_num = sim_id, scenario = rename_scenario( scenario ) ) %>%
    filter( death_rate == ifr_match( scenario )) %>%
    rename( !!JHU_REMAP_COLS )
    
  sim_arrow  
}


#' 25th percentile
q25 <- function(x)
  return( quantile(x, 0.25))

#' 50th percentile
q50 <- function(x)
  return( quantile(x, 0.50))

#' 75th percentile
q75 <- function(x)
  return( quantile(x, 0.75))


#' Generate state-level summary statistics from simulation paths
#'
#' Returns a tibble by scenario / date which summarizes statistics across
#' simulation paths

generate_state_summary <- function( jhu_df )
{
  print( paste("INFO: Summarizing state level statistics for simulation" ) )
  
  # net across geoid
  state_summary <- jhu_df %>% group_by( scenario,file_num,time ) %>% 
    select( scenario, file_num, time, all_of(DATA_OUTPUT_COLS))%>%
    summarize_all( list(sum))
  
  # aggregate over file_num
  state_summary <- state_summary %>% 
    group_by( scenario, time ) %>% 
    select( scenario,time,all_of(DATA_OUTPUT_COLS))%>%
    summarize_all( list( mean = mean, median=median, q25 = q25, q75 = q75)) %>%
    ungroup
  
  # this is the column order from the legacy python script
  col_order<-kronecker(DATA_OUTPUT_COLS, OUTPUT_SUFFIXES, FUN = paste0)
  state_summary <- state_summary %>% 
    select( scenario, time, all_of(col_order)) %>%
    arrange( scenario, time ) 
  
  return( state_summary )  
}

#' Generate county-level summary statistics from simulation paths
#'
#' Returns a tibble by scenario / county FIPS / date which summarizes statistics across
#' simulation paths

generate_county_summary <- function( jhu_df )
{
  print( paste("INFO: Summarizing county level statistics for simulation" ) )
  
  # net up to county level
  county_summary <- jhu_df %>% group_by( scenario,file_num,time, geoid ) %>% 
    select( scenario,file_num,time,geoid,all_of(DATA_OUTPUT_COLS))%>%
    summarize_all( list(sum))
  
  # aggregate over file_num
  county_summary <-county_summary %>% 
    group_by( scenario, time, geoid ) %>% 
    select( scenario,time, geoid, all_of(DATA_OUTPUT_COLS))%>%
    summarize_all( list( mean = mean, median=median, q25 = q25, q75 = q75)) %>%
    ungroup
  
  # this is the column order from the legacy python script
  col_order<-kronecker(DATA_OUTPUT_COLS, OUTPUT_SUFFIXES, FUN = paste0)
  county_summary <- county_summary %>% 
    select( scenario, time, geoid, all_of(col_order)) %>%
    arrange( scenario, time, geoid ) 
  
  return( county_summary )  
}


#' Read all JHU r0 modifiers, create r0 curves, account for the susceptible population for r-effective,
#' generate summary stats for the state and each county
#' 
#'
generate_reff_summary <- function( inputloc, rundate )
{
  inputloc <- expand_home_dir( inputloc )
  
  reffs <- tibble()
  
  spar<-arrow::open_dataset(file.path( inputloc, rundate, "spar"), partitioning = PARTITIONING) %>% collect()
  for( scenario in unique(spar$scenario))
  {    
    cat( "*****", scenario )

    reffs <- generate_reff_scenario( scenario, inputloc, rundate ) %>%
      bind_rows( reffs )
  }

  reffs
}
  
#' 
#' Create Reff curve for one scenario
#' 

generate_reff_scenario <- function( scenario, inputloc, rundate )
{
  print( paste("INFO: Summarizing reff curve statistics for simulation, scenario = ", scenario ) )
  
  geodata_file <- "geodata.csv" # df with county FIPS ('geoid') and population
  
  mtr_periods <- 10 # maximum number of periods a non-contiguous intervention is applied in any one county 
  
  # Download df with county FIPS ('geoid') and population columns
  
  geodata<-read_csv( geodata_file, col_types=cols(geoid=col_character())) %>%
    mutate(geoid=str_pad(geoid, 5, pad = "0"))%>%
    rename(pop=starts_with("pop"))
  
  # Download cumulative infections
  
  cumI<-arrow::open_dataset(file.path(inputloc, rundate,"hosp"), 
                            partitioning = PARTITIONING) %>% 
    dplyr::filter(lik_type=="global",
                  is_final=="final", 
                  scenario==!!scenario,
                  geoid %in% geodata$geoid) %>%
    dplyr::select(geoid, time, death_rate, scenario, location, incidI, sim_id) %>%
    dplyr::collect() %>%
    dplyr::mutate(sim_id = str_remove(sim_id, "\\..+$"), 
                  sim_id = as.numeric(sim_id),
                  time=as.Date(time))
  
  
  cumI<-cumI %>%
    group_by(geoid, death_rate, scenario, location, sim_id) %>%
    mutate(cum_inf=cumsum(incidI)) %>%
    ungroup()
  
  # Download NPI estimates
  
  spar <- arrow::open_dataset(file.path(inputloc, rundate,'spar'), 
                              partitioning = PARTITIONING) %>%
    dplyr::filter(lik_type=="global",
                  is_final=="final",
                  scenario==!!scenario, 
                  parameter=="R0") %>%
    dplyr::collect() %>% 
    dplyr::mutate(sim_id = str_remove(sim_id, "\\..+$"), 
                  sim_id = as.numeric(sim_id)) %>%
    dplyr::select(r0=value, location, scenario, death_rate, date, sim_id)
  
  snpi<- arrow::open_dataset(file.path(inputloc, rundate,'snpi'), 
                             partitioning = PARTITIONING) %>%
    dplyr::filter(lik_type=="global",
                  is_final=="final",
                  scenario==!!scenario,
                  geoid %in% geodata$geoid) %>%
    dplyr::filter(parameter=="r0") %>%
    dplyr::collect() %>%
    dplyr::mutate(sim_id = str_remove(sim_id, "\\..+$"), 
                  sim_id = as.numeric(sim_id)) %>%
    dplyr::select(-parameter)
  
  npi <- snpi %>%
    dplyr::left_join(spar) %>%
    dplyr::select(-date)
  
  # Expand non-contiguoous interventions to each time period has a row
  
  mtr_start <- npi %>%
    dplyr::select(geoid, scenario, death_rate, starts_with("npi"), start_date) %>%
    dplyr::distinct() %>%
    tidyr::separate(start_date, into = as.character(c(1:mtr_periods)), sep=",")
  
  mtr_end <- npi %>%
    dplyr::select(geoid, scenario, death_rate, starts_with("npi"), end_date) %>%
    dplyr::distinct() %>%
    tidyr::separate(end_date, into = as.character(c(1:mtr_periods)), sep=",")
  
  xx <- tibble()
  
  for(i in 1:mtr_periods){
    
    xx<-mtr_start %>%
      dplyr::select(geoid, scenario, death_rate, starts_with("npi"), start_date=as.symbol(i)) %>%
      dplyr::left_join(mtr_end %>%
                         dplyr::select(geoid, scenario, death_rate, starts_with("npi"), end_date=as.symbol(i))) %>%
      tidyr::drop_na() %>%
      dplyr::right_join(npi%>%
                          dplyr::select(-start_date, -end_date)) %>%
      tidyr::drop_na() %>%
      dplyr::mutate(across(ends_with("date"), ~lubridate::ymd(.x)))%>%
      dplyr::bind_rows(xx)
  }
  
  npi <- xx
  
  # Estimate daily Rt
  
  geoiddate<-crossing(geoid=geodata$geoid, time=seq(min(as.Date(npi$start_date)), max(as.Date(npi$end_date)), 1))
  
  daily_r<-list()
  
  for(i in 1:length(geodata$geoid)){
    daily_r[[i]]<-npi %>%
      filter(geoid == geodata$geoid[i])%>%
      left_join(geoiddate)%>%
      mutate(sim_id=if_else(start_date>time | end_date<time, NA_real_, sim_id))%>%
      drop_na() %>%
      group_by(geoid, sim_id, time, death_rate, scenario, location) %>%
      mutate(reduction=1-reduction)%>%
      summarize(reduction=prod(reduction),
                r0=unique(r0)) %>%
      mutate(rt=reduction*r0)
  }
  
  daily_r<-bind_rows(daily_r) %>%
    ungroup()
  
  rc<-cumI %>%
    dplyr::left_join(geodata) %>%
    dplyr::right_join(daily_r) %>%
    dplyr::group_by(scenario, time, location) %>%
    dplyr::mutate(rt=rt*(1-cum_inf/pop), 
                  weight=pop/sum(pop))
  
  # Summarize to get mean and quantiles
  
  #probs <- c(0.01, 0.025, seq(0.05, 0.95, by = 0.05), 0.975, 0.99))
  probs <- c( 0.1, 0.25, 0.5, 0.75, 0.9)
  rc_state <- rc %>%
    dplyr::group_by(scenario, time, death_rate, location) %>%
    dplyr::summarize(x=list(enframe(c(Hmisc::wtd.quantile(rt, weights=weight, normwt=TRUE, probs=probs),
                                      mean=Hmisc::wtd.mean(rt, weights=weight, normwt=TRUE)),
                                    "quantile","Rt"))) %>%
    unnest(x) %>%
    rename(geoid=location)
  
  rc <-rc %>%
    dplyr::group_by(scenario, time, death_rate, geoid) %>%
    dplyr::summarize(x=list(enframe(c(quantile(rt, probs=probs),
                                      mean=mean(rt)),
                                    "quantile", "Rt"))) %>%
    unnest(x) %>%
    dplyr::bind_rows(rc_state) 
  
  rc_wider <- rc %>% 
    pivot_wider( names_from = "quantile", names_prefix = "r_eff_q", values_from = Rt )%>% 
    rename_with( ~ gsub("%", "", .x, fixed = TRUE )) %>%
    rename( r_eff_median = r_eff_q50, r_eff_mean = r_eff_qmean)
  
  rc_wider
}

#' Save a summary statistics to appropriate CSVs
#'
#' Saves one csv per scenario
#' 

save_csv_by_scenario <- function( summary_df, outputloc, rundate, suffix = NULL )
{
  msg <- "INFO: Writing summary statistics to csv" 
  if( !is.null(suffix) )
    msg <- paste( msg, "( suffix = ",suffix, ")" )
  print( msg )
  
  scenarios <- unique( summary_df$scenario )
  if( length( scenarios ) == 0 )
    stop( "ERROR: no scenarios found - do input files line up with scenarios?" )
  
  for( scenario in scenarios)
  {
    write_me <- summary_df %>% filter( scenario == !!scenario ) %>% select( -scenario )
    
    filename<- str_replace_all( scenario, " ", "_")
    if( !is.null( suffix))
      filename <- paste( filename, suffix, sep=".")
    if( !file.exists( file.path(outputloc,rundate)))
      dir.create(file.path(outputloc,rundate))

    filename <- paste( filename, "csv", sep=".")
    write_csv( write_me, file.path( outputloc, rundate, filename )) 
  }
  invisible( summary_df )
}

#' Detect whether this rundate is the most recent rundate
#'

is_latest <- function( outputloc, rundate = RUNDATE )
{
  files <- list.files( outputloc )
  datelike <- files[ str_detect(files, "^[1-9][0-9]{7}$" ) ]

  return( max(datelike) == rundate )
}


#' Load simulation scenarios from raw files, process, and save
#'
#' do_counties controls whether to make summaries by county in addition to state summary
#' 

process_jhu_simulation <- function( inputloc, outputloc, rundate = RUNDATE, do_counties=TRUE )
{
  jhu_simulation <- read_jhu_simulation( inputloc, rundate=rundate )
  
  state_summary <- generate_state_summary( jhu_simulation )
  save_csv_by_scenario( state_summary, outputloc, rundate )
  
  if( do_counties )
  {
    county_summary <- generate_county_summary( jhu_simulation )
    save_csv_by_scenario( county_summary, outputloc, rundate, "county" )
  }
  
  if( ymd(rundate) >= ymd("20200623"))
  {
    reff_summary <- generate_reff_summary(  inputloc, rundate )
    save_csv_by_scenario( reff_summary, outputloc, rundate, "reff" )
  }
  
  invisible(inputloc)  
}


