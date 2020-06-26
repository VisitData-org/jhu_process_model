#!/usr/bin/env Rscript

source( "jhu_covid.R" )

#' Create argparse object
#'

setup_argparse <- function()
{
  # create parser object
  parser <- ArgumentParser()
  
  # specify our desired options 
  # by default ArgumentParser will add an help option 
  
  parser$add_argument('-i', '--input', metavar='inputdir', dest='input', action="store",required=TRUE,
                      help='input directory to read graph_data from')
  parser$add_argument('-o','--output',metavar='ouputdir',dest='output',action="store",required=TRUE,
                      help='base output directory')
  parser$add_argument('--add_counties',action='store_true',default=TRUE,
                      help="set to add county-level data to outputs")
  parser$add_argument('--start_from_csvs',action='store_true',default=FALSE,
                      help="set to skip all data loading and just write to s3")
  parser$add_argument('-d',metavar='YYYYMMDD',dest='rundate',default=RUNDATE,action="store",
                      help='run date')
  
  return(parser)
}

parser <- setup_argparse()
args <- parser$parse_args()

if( !args$start_from_csvs)
{
  process_jhu_simulation(args$input, args$output, rundate=args$rundate, do_counties = args$add_counties)  
}

upload_to_s3(args$output, rundate=args$rundate )
if( is_latest( args$output, args$rundate ) )
  upload_to_s3(args$output, rundate=args$rundate, latest=TRUE )
