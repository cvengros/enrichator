require 'gooddata'
require './csv_ads'

include GoodData::Bricks

p = GoodData::Bricks::Pipeline.prepare([
  LoggerMiddleware,
  BenchMiddleware,
  S3DownloadMiddleware,
  ADSLoadDataMiddleware,
  ExecuteBrick
])

p.call($SCRIPT_PARAMS)