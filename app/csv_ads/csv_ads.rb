require 'gooddata'

module GoodData
  module Bricks
    class S3DownloadMiddleware < Bricks::Middleware
      def call(params)
        # connect to s3
        config = params['config']
        bucket_name = config['s3_bucket_name']
        s3 = AWS::S3.new(
          :access_key_id => config['aws_access_key_id'],
          :secret_access_key => config['aws_secret_access_key']
        )

        bucket = s3.buckets[bucket_name]
        # download all files in the given folder
        # TODO: chytristika

        # make temporary folder
        temp_dir = Dir.mktmpdir

        # list all files in the s3 folder
        s3_folder = config['s3_base_path'] || ''

        bucket.objects.with_prefix(s3_folder).each do |obj|
          # download to the temporary folder
          filename = File.join(temp_dir, obj.key.split('/')[-1])
          File.open(filename, 'wb') do |file|
            obj.read do |chunk|
               file.write(chunk)
            end
          end
        end
        params['data_dir'] = temp_dir
        params
      end
    end
    class ADSCreateTablesMiddleware < Bricks::Middleware
      def call(params)
      end
    end
    class ADSLoadDataMiddleware < Bricks::Middleware
      def call(params)
      end
    end
    class ExecuteBrick
      def call(params)
      end
    end
  end
end