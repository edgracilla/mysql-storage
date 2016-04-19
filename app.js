'use strict';

var async         = require('async'),
	moment        = require('moment'),
	platform      = require('./platform'),
	isNil         = require('lodash.isnil'),
	isEmpty       = require('lodash.isempty'),
	isNumber      = require('lodash.isnumber'),
	isString      = require('lodash.isstring'),
	isBoolean     = require('lodash.isboolean'),
	isPlainObject = require('lodash.isplainobject'),
	isArray       = require('lodash.isarray'),
	tableName, fieldMapping, pool;

let insertData = function (data, callback) {
	pool.getConnection((connectionError, connection) => {
		if (connectionError) return callback(connectionError);

		connection.query(`INSERT INTO ${tableName} SET ?`, data, (insertError, result) => {
			connection.release();

			if (!insertError) {
				platform.log(JSON.stringify({
					title: 'Record Successfully inserted to MySQL.',
					data: result
				}));
			}

			callback(insertError);
		});
	});
};

let processData = function (data, callback) {
	let saveData = {};

	async.forEachOf(fieldMapping, (field, key, done) => {
		let datum = data[field.source_field],
			processedDatum;

		if (!isNil(datum) && !isEmpty(field.data_type)) {
			try {
				if (field.data_type === 'String') {
					if (isPlainObject(datum))
						processedDatum = JSON.stringify(datum);
					else
						processedDatum = `${datum}`;
				}
				else if (field.data_type === 'Integer') {
					if (isNumber(datum))
						processedDatum = datum;
					else {
						let intData = parseInt(datum);

						if (isNaN(intData))
							processedDatum = datum; //store original value
						else
							processedDatum = intData;
					}
				}
				else if (field.data_type === 'Float') {
					if (isNumber(datum))
						processedDatum = datum;
					else {
						let floatData = parseFloat(datum);

						if (isNaN(floatData))
							processedDatum = datum; //store original value
						else
							processedDatum = floatData;
					}
				}
				else if (field.data_type === 'Boolean') {
					if (isBoolean(datum))
						processedDatum = datum;
					else {
						if ((isString(datum) && datum.toLowerCase() === 'true') || (isNumber(datum) && datum === 1))
							processedDatum = true;
						else if ((isString(datum) && datum.toLowerCase() === 'false') || (isNumber(datum) && datum === 0))
							processedDatum = false;
						else
							processedDatum = (datum) ? true : false;
					}
				}
				else if (field.data_type === 'Date' || field.data_type === 'DateTime') {
					if (moment(datum).isValid() && isEmpty(field.format))
						processedDatum = datum;
					else if (moment(datum).isValid() && !isEmpty(field.format))
						processedDatum = moment(datum).format(field.format);
					else
						processedDatum = datum;
				}
			}
			catch (e) {
				if (isPlainObject(datum))
					processedDatum = JSON.stringify(datum);
				else
					processedDatum = datum;
			}
		}
		else if (!isNil(datum) && isEmpty(field.data_type)) {
			if (isPlainObject(datum))
				processedDatum = JSON.stringify(datum);
			else
				processedDatum = `${datum}`;
		}
		else
			processedDatum = null;

		saveData[key] = processedDatum;

		done();
	}, () => {
		callback(null, saveData);
	});
};

platform.on('data', function (data) {
	if (isPlainObject(data)) {
		processData(data, (error, processedData) => {
			insertData(processedData, (error) => {
				if (error) platform.handleException(error);
			});
		});
	}
	else if (isArray(data)) {
		async.each(data, function (datum) {
			processData(datum, (error, processedData) => {
				insertData(processedData, (error) => {
					if (error) platform.handleException(error);
				});
			});
		});
	}
	else
		platform.handleException(new Error(`Invalid data received. Data must be a valid Array/JSON Object or a collection of objects. Data: ${data}`));
});

/*
 * Event to listen to in order to gracefully release all resources bound to this service.
 */
platform.on('close', function () {
	let d = require('domain').create();

	d.once('error', (error) => {
		console.error(error);
		platform.handleException(error);
		platform.notifyClose();
		d.exit();
	});

	d.run(() => {
		pool.end((error) => {
			if (error) platform.handleException(error);
			platform.notifyClose();
			d.exit();
		});
	});
});

/*
 * Listen for the ready event.
 */
platform.once('ready', function (options) {
	async.waterfall([
		async.constant(options.field_mapping || '{}'),
		async.asyncify(JSON.parse),
		(obj, done) => {
			fieldMapping = obj;
			done();
		}
	], (parseError) => {
		if (parseError) {
			platform.handleException(new Error('Invalid field mapping. Must be a valid JSON String.'));

			return setTimeout(() => {
				process.exit(1);
			}, 2000);
		}

		let isEmpty = require('lodash.isempty');

		async.forEachOf(fieldMapping, (field, key, callback) => {
			if (isEmpty(field.source_field))
				callback(new Error(`Source field is missing for ${key} in field mapping.`));
			else if (field.data_type && (field.data_type !== 'String' &&
				field.data_type !== 'Integer' && field.data_type !== 'Float' &&
				field.data_type !== 'Boolean' && field.data_type !== 'Date' &&
				field.data_type !== 'DateTime')) {

				callback(new Error(`Invalid Data Type for ${key} in field mapping. Allowed data types are String, Integer, Float, Boolean, Date, DateTime.`));
			}
			else
				callback();
		}, (error) => {
			if (error) {
				console.error('Error parsing field mapping.', error);
				platform.handleException(error);

				return setTimeout(() => {
					process.exit(1);
				}, 2000);
			}

			var mysql = require('mysql');

			tableName = options.table;

			pool = mysql.createPool({
				host: options.host,
				port: options.port,
				user: options.user,
				password: options.password,
				database: options.database,
				acquireTimeout: 15000
			});

			pool.on('error', (mysqlError) => {
				platform.handleException(mysqlError);
			});

			platform.log('MySQL Storage initialized.');
			platform.notifyReady();
		});
	});
});
