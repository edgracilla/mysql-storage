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
	tableName, parseFields, pool;

let insertData = function (data, callback) {
	pool.getConnection((connectionError, connection) => {
		if (connectionError) return callback(connectionError);

		connection.query(`INSERT INTO  ${tableName} SET ?`, data, (insertError, result) => {
			connection.release();

			if (insertError) {
				console.error('Failed to save record in MySQL.', insertError);
				platform.handleException(insertError);
			}
			else {
				platform.log(JSON.stringify({
					title: 'Record Successfully inserted to MySQL.',
					data: result
				}));
			}

			callback(insertError);
		});
	});
};

/*
 * Listen for the data event.
 */
platform.on('data', function (data) {
	let saveData = {};

	async.forEachOf(parseFields, (field, key, callback) => {
		let datum = data[field.source_field],
			processedDatum;

		if (!isNil(datum) && !isEmpty(field.data_type)) {
			try {
				if (field.data_type === 'String') {
					if (isPlainObject(datum))
						processedDatum = JSON.stringify(datum);
					else
						processedDatum = ''.concat(datum);
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
						if ((isString(datum) && datum.toLocaleLowerCase() === 'true') || (isNumber(datum) && datum === 1))
							processedDatum = true;
						else if ((isString(datum) && datum.toLocaleLowerCase() === 'false') || (isNumber(datum) && datum === 0))
							processedDatum = false;
						else
							processedDatum = datum;
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

				console.error('Data conversion error in MySQL.', e);
				platform.handleException(e);
			}
		}
		else if (!isNil(datum) && isEmpty(field.data_type)) {
			if (isPlainObject(datum))
				processedDatum = JSON.stringify(datum);
			else
				processedDatum = datum;
		}
		else
			processedDatum = null;

		saveData[key] = processedDatum;

		callback();
	}, () => {
		insertData(saveData, (error) => {
			if (error) platform.handleException(error);
		});
	});
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
	let isEmpty = require('lodash.isempty');

	try {
		parseFields = JSON.parse(options.fields);
	}
	catch (ex) {
		platform.handleException(new Error('Invalid option parameter: fields. Must be a valid JSON String.'));

		return setTimeout(() => {
			process.exit(1);
		}, 2000);
	}

	async.forEachOf(parseFields, (field, key, callback) => {
		if (isEmpty(field.source_field))
			callback(new Error(`Source field is missing for ${key} in the fields configuration parameter.`));
		else if (field.data_type && (field.data_type !== 'String' &&
			field.data_type !== 'Integer' && field.data_type !== 'Float' &&
			field.data_type !== 'Boolean' && field.data_type !== 'Date' &&
			field.data_type !== 'DateTime')) {

			callback(new Error(`Invalid Data Type for ${key} allowed data types are (String, Integer, Float, Boolean, Date, DateTime) in the fields mapping configuration parameter.`));
		}
		else
			callback();
	}, (error) => {
		if (error) {
			console.error('Error parsing fields mapping configuration for MySQL Plugin.', error);
			platform.handleException(new Error('Error parsing fields mapping configuration for MySQL Plugin.'));

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
			acquireTimeout: 20000

		});

		pool.on('error', (mysqlError) => {
			platform.handleException(mysqlError);
		});

		platform.log('MySQL Storage initialized.');
		platform.notifyReady();
	});
});
