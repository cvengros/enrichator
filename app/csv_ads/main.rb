require 'gooddata'
require './csv_ads'

include GoodData::Bricks

p = GoodData::Bricks::Pipeline.prepare([
  LoggerMiddleware,
  BenchMiddleware,
  S3DownloadMiddleware,
  ADSCreateTablesMiddleware,
  ADSLoadDataMiddleware,
  ExecuteBrick
])

p.call($SCRIPT_PARAMS)