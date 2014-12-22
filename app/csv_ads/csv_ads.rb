require 'gooddata'
require 'gooddata_datawarehouse'

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
        # TODO: chytristika - last load

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
        puts "Data downloaded to #{temp_dir}"
        @app.call(params)
      end
    end
    class ADSLoadDataMiddleware < Bricks::Middleware
      def call(params)
        config = params['config']
        data_dir = params['data_dir']
        Dir.glob(File.join(data_dir, "*")).each do |csv_file|
          # for each csv create a "temporary" table in ADS,


          table_name = File.basename(csv_file).split('.')[0].gsub(/[\s"-]/,'')
          temp_table_name = "temp#{rand(100000)}_#{table_name}"

          dwh = GoodData::Datawarehouse.new(config['ads_username'], config['ads_password'], config['ads_instance_id'])

          # create the temp table
          cols = dwh.create_table_from_csv_header(temp_table_name, csv_file)

          if cols.empty?
            puts "#{csv_file} is empty, skipping..."
            next
          end

          # load the data there
          dwh.load_data_from_csv(temp_table_name, csv_file, :columns => cols)

          # drop the current table
          dwh.drop_table(table_name, :skip_if_exists => true)

          # rename the temporary to the right name
          dwh.rename_table(temp_table_name, table_name)

        end
        @app.call(params)
      end
    end
    class ExecuteBrick
      def call(params)
        puts 'hhah'

      end
    end
  end
end
