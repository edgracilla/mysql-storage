# MySQL Storage

[![Build Status](https://travis-ci.org/Reekoh/mysql-storage.svg)](https://travis-ci.org/Reekoh/mysql-storage)
![Dependencies](https://img.shields.io/david/Reekoh/mysql-storage.svg)
![Dependencies](https://img.shields.io/david/dev/Reekoh/mysql-storage.svg)
![Built With](https://img.shields.io/badge/built%20with-gulp-red.svg)

MySQL Storage Plugin for the Reekoh IoT Platform.

## Assumptions:

1. Data would be in JSON format
2. Data would be processed based on configuration format
3. Conversions and formatting are done within Reekoh only minimal conversions are done in the plugin
4. Field configuration is correctly done for the specified table

## Process

1. Data would be written directly to the mysql host specified
2. Storage plugin will only write data using plain SQL-Insert statement
3. All errors will be logged and no data should be written
4. Data will be parsed accordingly based on field configuration

## Field Configuration

1. Input for this field is in JSON format {"(field_name)" : {"source_field" : "value", "data_type": "value", "format": "value"}}.
2. field_name will be the name of the column in the mysql Table
3  source_field (required) value will be the name of the field in the JSON Data passed to the plugin
4  data_type there are 6 available data types that will convert data to it's proper type before saving
   we have String, Integer, Float, Boolean, Date and DateTime leaving this blank will just use the current data for the field
5. format is only available for DateTime data_type this will allow users to format the date/time before saving
   i.e. (YYYY-MM-DD HH:mm:ss) kindly refer to the [MomentJS docs](http://momentjs.com/docs/) for more details.
   of mysql
6. JSON Data is not supported as a data_type but you can save it if there is a field in MySQL

```javascript
{
  "co2_field": {
	"source_field": "co2",
	"data_type": "String"
  },
  "temp_field": {
	"source_field": "temp",
	"data_type": "Integer"
  },
  "quality_field": {
	"source_field": "quality",
	"data_type": "Float"
  },
  "metadata_field": {
	"source_field": "metadata",
	"data_type": "JSON"
  },
  "reading_time_field": {
	"source_field": "reading_time",
	"data_type": "DateTime",
	"format": "yyyy-MM-ddTHH:mm:ss"
  },
  "random_data_field": {
	"source_field": "random_data"
  },
  "is_normal_field": {
	"source_field": "is_normal",
	"data_type": "Boolean"
  }
}
```

### Sample Data:

```javascript
{
  co2: '11%',
  temp: 23,
  quality: 11.25,
  metadata: '{"name": "warehouse air conditioning"}',
  reading_time: '2015-11-27T11:04:13.539Z',
  random_data: 'abcdefg',
  is_normal: true
}
```

### MySQL Fields:

Type Field mapping |
-------------------|
_id                |
co2_field          |
temp_field         |
quality_field      |
metadata_field     |
reading_time_field |
random_data_field  |
is_normal_field    |
