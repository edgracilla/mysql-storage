'use strict';

const HOST     = 'reekoh-mysql.cg1corueo9zh.us-east-1.rds.amazonaws.com',
	  PORT     = 3306,
	  USER     = 'reekoh',
	  PASSWORD = 'rozzwalla',
	  DATABASE = 'reekoh',
	  TABLE    = 'reekoh_table',
	  _ID      = new Date().getTime();

var cp     = require('child_process'),
	assert = require('assert'),
	should = require('should'),
	moment = require('moment'),
	storage;

var record = {
	_id: _ID,
	co2: '11%',
	temp: 23,
	quality: 11.25,
	power: 90,
	reading_time: '2015-11-27T11:04:13.539Z',
	metadata: '{"metadata_json": "reekoh metadata json"}',
	random_data: 'abcdefg',
	is_normal: true
};

describe('Storage', function () {
	this.slow(8000);

	after('terminate child process', function () {

		storage.send({
			type: 'close'
		});

		setTimeout(function () {
			storage.kill('SIGKILL');
		}, 4000);
	});

	describe('#spawn', function () {
		it('should spawn a child process', function () {
			assert.ok(storage = cp.fork(process.cwd()), 'Child process not spawned.');
		});
	});

	describe('#handShake', function () {
		it('should notify the parent process when ready within 8 seconds', function (done) {
			this.timeout(8000);

			storage.on('message', function (message) {
				if (message.type === 'ready')
					done();
			});

			storage.send({
				type: 'ready',
				data: {
					options: {
						host: HOST,
						port: PORT,
						user: USER,
						password: PASSWORD,
						database: DATABASE,
						table: TABLE,
						field_mapping: JSON.stringify({
							_id: {source_field: '_id', data_type: 'Integer'},
							co2_field: {source_field: 'co2', data_type: 'String'},
							temp_field: {source_field: 'temp', data_type: 'Integer'},
							quality_field: {source_field: 'quality', data_type: 'Float'},
							'Power(KW)': {source_field: 'power', data_type: 'Integer'},
							reading_time_field: {
								source_field: 'reading_time',
								data_type: 'DateTime',
								format: 'YYYY-MM-DDTHH:mm:ss'
							},
							metadata_field: {source_field: 'metadata', data_type: 'String'},
							random_data_field: {source_field: 'random_data'},
							is_normal_field: {source_field: 'is_normal', data_type: 'Boolean'}
						})
					}
				}
			}, function (error) {
				assert.ifError(error);
			});
		});
	});

	describe('#data', function () {
		it('should process the data', function (done) {
			storage.send({
				type: 'data',
				data: record
			}, done);
		});
	});

	describe('#data', function () {
		it('should have inserted the data', function (done) {
			this.timeout(10000);

			var mysql = require('mysql');

			var connection = mysql.createConnection({
				host: HOST,
				port: PORT,
				user: USER,
				password: PASSWORD,
				database: DATABASE
			});

			connection.connect(function (err) {
				assert.ifError(err);

				setTimeout(() => {
					connection.query('SELECT * FROM ' + TABLE + ' WHERE _id = ' + _ID, function (error, result, fields) {
						should.exist(result[0]);
						var resp = result[0];

						//cleanup for JSON stored string
						var cleanMetadata = resp.metadata_field.replace(/\\"/g, '"');
						var str = JSON.stringify(record.metadata);
						var str2 = JSON.stringify(cleanMetadata);

						should.equal(record.co2, resp.co2_field, 'Data validation failed. Field: co2');
						should.equal(record.temp, resp.temp_field, 'Data validation failed. Field: temp');
						should.equal(record.quality, resp.quality_field, 'Data validation failed. Field: quality');
						should.equal(record.random_data, resp.random_data_field, 'Data validation failed. Field: random_data');
						should.equal(moment(record.reading_time).format('YYYY-MM-DD HH:mm:ss'),
							moment(resp.reading_time_field).format('YYYY-MM-DD HH:mm:ss'),
							'Data validation failed. Field: reading_time');
						should.equal(str, str2, 'Data validation failed. Field: metadata');

						done();
					});
				}, 4000);
			});
		});
	});
});