# DevOps Scripts

## InfluxDB Scripts
Construct multiple measurement from a single measurement. Script written in ruby v2.4.0
### Usage:
1) Install Gem dependencies (Included Gemfile)
2) Modify the configuration file & add JSON for mapping feilds to measurement as shown.
3) Execute the Ruby script measurement-migration-script.rb
### Configuration
Use the `'migrate.conf'` for configuration.

[connection]\
host: InfluxDB Host to connect\
port: InfluxDB Port\
database = InfluxDB Database\
username = "username"\
password = "password"

[migrate]\
measurement = "measurement_name_to_split"\
start_from_previous_migration_date = true (Not used in script, can be Ignored)
### Measurement JSON
Its requires measurement JSON used for mapping fields to measurement name. Filename should be <<measurement_name_to_split>>.json
```
{
  "measurement1": ["idletime", "field1"],
  "measurement2": ["uptime", "field3"]
}
```
Example: field with 'uptime', 'field3' will be inserted into the measurement name 'measurement2'\
         and field with 'idletime', 'field1' will be inserted into the measurement name 'measurement1'
