require 'influxdb'
require 'json'
require 'toml'

config = TOML.load_file('migrate.conf')
puts config['connection']
$influxdb = InfluxDB::Client.new config['connection']['database'],
                                host: config['connection']['host'],
                                port: config['connection']['port'],
                                use_ssl: config['connection']['use_ssl'],
                                verify_ssl: config['connection']['verify_ssl'],
                                ssl_ca_cert: config['connection']['ssl_ca_cert'],
                                retry: config['connection']['retry'],
                                username: config['connection']['username'],
                                password: config['connection']['password'],
                                epoch: 's'

def get_start_date
  # Store in Date filename
  filename = 'Date'
  File.file?(filename) ? File.read(filename) : nil
end

def get_migration_json(filename)
  JSON.parse(File.read("#{filename}.json"))
end

def execute_query(query)
  $influxdb.query query
end

def extract_tags(measurement_name)
  # To extract tags
  query = "SHOW TAG KEYS FROM #{measurement_name}"
  result = execute_query(query)
  list = result.collect { |tags| tags['values'] }.flatten unless result.nil?
  tag_keys = list.collect { |t| t['tagKey'] }.compact.uniq unless list.nil?
end

def migrate(measurement_name)

  # Query results
  query = "select * from #{measurement_name}"
  start_date = get_start_date
  query += " where time >= '#{start_date}'" if start_date
  tags = extract_tags(measurement_name)

  #  Format for writing
  migration_fields = get_migration_json(measurement_name)
  $influxdb.query query do |_, _, points|
    points.each do |row|
      point_set = generate_custom_measurement_hash(row, migration_fields, tags)
      send_to_influxdb(point_set)
    end
  end
  #write to custom measurement
  
  #error:
  #update date
end

def generate_custom_measurement_hash(row, migration_fields, measurement_tags)
  point_set = {} # Used to store the tags & field set in the format point_set = {<measurement_name1>: { tags: {}, fields: {}, timestamp:<timestamp>}, <measurement_name2>: { tags: {}, fields: {}, timestamp: <timestamp>}, ... }
  row.each do |k,v|
    unless measurement_tags.include?(k) || k == 'time' || v.nil? # fields
      measurement_name = custom_measurement_of_field(k, migration_fields)
      if measurement_name
        point_set[measurement_name] ||= {}

        tags = {}
        measurement_tags.each do |t|
          tags[t] = row[t] if row.keys.include? t
        end

        point_set[measurement_name][:tags] ||= tags unless tags.empty?
        point_set[measurement_name][:values] ||= {}
        val = is_number?(v) ? v.to_f : v
        point_set[measurement_name][:values][k] = val
        point_set[measurement_name][:timestamp] = row['time']
      end
    end
  end
  point_set
end

def custom_measurement_of_field(f, migration_fields)
  measurement_name = nil
  migration_fields.each do |k,v|
    measurement_name = k if v.include? f
  end
  measurement_name
end

def send_to_influxdb(point_set)
  point_set.keys.each do |m|
    $influxdb.write_point(m, point_set[m])
  end
end

def is_number?(input)
  true if Float(input) rescue false
end

migrate(config['migrate']['measurement'])
