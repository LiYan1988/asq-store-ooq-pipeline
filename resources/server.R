# Title     : investigate number of sizes in OOQ
# Objective : TODO
# Created by: liynx
# Created on: 2021-05-12
# Launch the R Agent.
# If you include the last line you are in server mode.
# If not, you have initialized a working environment without the polling.
# That is handy for debugging or educational purposes, refer to pipeline.R
setwd("/Users/liynx/working/ooq/asq-store-ooq-1261-size-number-experiments/experiment-2/assortment/r_agent")
# Import system libraries.
library(Matrix)
# Import third-party libraries.
library(dplyr)
library(futile.logger)
#library(gurobi)
library(jsonlite)
#library(lpSolveAPI)
library(openssl)
library(R.utils)
library(reticulate)
library(RProtoBuf)
library(tidyverse)
library(zoo)
# Import server_helpers first.
source('r_agent/utils/date_utils.R')
source('r_agent/utils/enums.R')
source('r_agent/utils/time_utils.R')
source('r_agent/utils/validation_utils.R')
source("r_agent/server_helpers/adapter_redis.R")
source("r_agent/server_helpers/environment_variables.R")
source("r_agent/server_helpers/lib_log.R")
source("r_agent/server_helpers/lib_redis.R")
source("r_agent/server_helpers/poll.R")
source("r_agent/server_helpers/queue.R")
source("r_agent/server_helpers/timing.R")
source("r_agent/server_helpers/trace.R")
# IMPORTANT: This needs to happen before sourcing the business logic code files.
# Set server state e.g. ENVIRONMENT_VARIABLES, logging threshold and proto files.
ENVIRONMENT_VARIABLES = make_environment_variables()
PROTOBUF_ROOT = "../protobuf/"
RProtoBuf::readProtoFiles(paste0(PROTOBUF_ROOT, "/API/input.proto"))
RProtoBuf::readProtoFiles(paste0(PROTOBUF_ROOT, "/API/output.proto"))
RProtoBuf::readProtoFiles(paste0(PROTOBUF_ROOT, "/API/trace.proto"))
RProtoBuf::readProtoFiles(paste0(PROTOBUF_ROOT, "/API/inference.proto"))
# Instantiate a Redis Adapter to get jobs, save traces, etc.
REDIS_INSTANCE = Redis$new(ENVIRONMENT_VARIABLES$REDIS_HOST, ENVIRONMENT_VARIABLES$REDIS_PORT, ENVIRONMENT_VARIABLES$REDIS_PASSWORD)
REDIS_ADAPTER = RedisAdapter$new(REDIS_INSTANCE)
# Instantiate the necessary objects to be able to push traces, timings and logs to Redis.
TRACE = Trace$new()
TIMING = Timing$new()
init_log(ENVIRONMENT_VARIABLES$LOG_LEVEL)
# Import job, pipeline, modules and utils after.
source('r_agent/pipeline.R')
source('r_agent/job/asq_job.R')
source("r_agent/job/asq_job_reader.R")
source("r_agent/job/asq_job_writer.R")
source('r_agent/job/user_message.R')
source('r_agent/modules/module_extrapolation.R')
source('r_agent/modules/module_forecast.R')
source('r_agent/modules/module_forecast_lookback.R')
source('r_agent/modules/module_forecast_random_forest.R')
source('r_agent/modules/module_tdt.R')
source('r_agent/modules/module_r3a.R')
source('r_agent/modules/module_ooq.R')
source('r_agent/modules/module_ooq_helpers.R')
source('r_agent/modules/module_wdf.R')
source('r_agent/modules/module_ssim.R')
source('r_agent/wrappers/wrapper_ooq.R')
# Initialize Python OOQ.
# PYTHON_PATH = "/usr/bin/python3"
# Change to your system python path or specific python path of anaconda environment with `which python`
PYTHON_PATH = "/Users/liynx/anaconda3/envs/asq-store-ooq-1261/bin/python"
PY_OOQ_FILENAME = "r_agent/modules_python/asq-store-ooq/__main__.py" # clone https://bitbucket.hm.com/scm/haalasq/asq-store-ooq.git to the r_agent/r_agent/modules_python folder
use_python(PYTHON_PATH)
filename = PY_OOQ_FILENAME
source_python(filename)

filename = '/Users/liynx/working/ooq/asq-store-ooq-1261-size-number-experiments/experiment-2/input/f384e762-eeb2-eb11-94b3-501ac5e6ac5c.bin' # Path to downloaded binary file from cockpit, the input.bin
# filename = '/Users/liynx/working/ooq/asq-store-ooq-1261-size-number-experiments/experiment-1/input/0c160a7d-dfb7-eb11-94b3-501ac5e6ac5c.bin' # Path to downloaded binary file from cockpit, the input.bin
# Remove from stacktrace the call to the actual "traceback" function.
options(error=function()traceback(2))
# Log that server setup is completed.
flog.info("Server initialized.", scope = "Server", job_id = -1)
# Run this for server mode, do not run this for standalone execution.
#Poll$new(ENVIRONMENT_VARIABLES$REDIS_WORK_QUEUE, ENVIRONMENT_VARIABLES$POLL_TIME)$start_polling()
# binary_name <- substr(filename, 32, 46)
binary_name <- str_split(rev(str_split(filename, "/")[[1]])[1], "\\.")[[1]][1]
asq_job = AsqJob$new(filename = filename)
asq_job$feature_flags[["IS_PY_OOQ_ACTIVE"]] = TRUE
process_asq_job(asq_job = asq_job)

# change python path
# clone repo
# download input.bin
# start port forwarding
# run this file
# generate 
