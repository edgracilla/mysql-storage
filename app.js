'use strict'

const reekoh = require('reekoh')
const plugin = new reekoh.plugins.Storage()

const async = require('async')
const moment = require('moment')
const isNil = require('lodash.isnil')
const isEmpty = require('lodash.isempty')
const isNumber = require('lodash.isnumber')
const isString = require('lodash.isstring')
const isBoolean = require('lodash.isboolean')
const isPlainObject = require('lodash.isplainobject')

let tableName = null
let fieldMapping = null
let pool = null

let insertData = (data, callback) => {
  pool.getConnection((connectionError, connection) => {
    if (connectionError) return callback(connectionError)

    connection.query(`INSERT INTO ${tableName} SET ?`, data, (insertError, result) => {
      connection.release()

      if (!insertError) {
        plugin.log(JSON.stringify({
          title: 'Record Successfully inserted to MySQL.',
          data: result
        }))
      }

      if (!insertError) plugin.emit('processed')
      callback(insertError)
    })
  })
}

let processData = (data, callback) => {
  let processedData = {}

  async.forEachOf(fieldMapping, (field, key, done) => {
    let datum = data[field.source_field]
    let processedDatum = null

    if (!isNil(datum) && !isEmpty(field.data_type)) {
      try {
        if (field.data_type === 'String') {
          if (isPlainObject(datum)) {
            processedDatum = JSON.stringify(datum)
          } else {
            processedDatum = `${datum}`
          }
        } else if (field.data_type === 'Integer') {
          if (isNumber(datum)) {
            processedDatum = datum
          } else {
            let intData = parseInt(datum)

            if (isNaN(intData)) {
              processedDatum = datum
            } else {
              processedDatum = intData
            }
          }
        } else if (field.data_type === 'Float') {
          if (isNumber(datum)) {
            processedDatum = datum
          } else {
            let floatData = parseFloat(datum)

            if (isNaN(floatData)) {
              processedDatum = datum
            } else {
              processedDatum = floatData
            }
          }
        } else if (field.data_type === 'Boolean') {
          if (isBoolean(datum)) { processedDatum = datum } else {
            if ((isString(datum) && datum.toLowerCase() === 'true') || (isNumber(datum) && datum === 1)) {
              processedDatum = true
            } else if ((isString(datum) && datum.toLowerCase() === 'false') || (isNumber(datum) && datum === 0)) {
              processedDatum = false
            } else {
              processedDatum = !!datum
            }
          }
        } else if (field.data_type === 'Date' || field.data_type === 'DateTime' || field.data_type === 'Timestamp') {
          if (isEmpty(field.format) && moment(datum).isValid()) {
            processedDatum = moment(datum).toDate()
          } else if (!isEmpty(field.format) && moment(datum, field.format).isValid()) {
            processedDatum = moment(datum, field.format).toDate()
          } else if (!isEmpty(field.format) && moment(datum).isValid()) {
            processedDatum = moment(datum).toDate()
          } else {
            processedDatum = datum
          }
        }
      } catch (e) {
        if (isPlainObject(datum)) {
          processedDatum = JSON.stringify(datum)
        } else {
          processedDatum = datum
        }
      }
    } else if (!isNil(datum) && isEmpty(field.data_type)) {
      if (isPlainObject(datum)) {
        processedDatum = JSON.stringify(datum)
      } else {
        processedDatum = `${datum}`
      }
    } else {
      processedDatum = null
    }

    processedData[key] = processedDatum

    done()
  }, () => {
    callback(null, processedData)
  })
}

plugin.on('data', (data) => {
  if (isPlainObject(data)) {
    processData(data, (error, processedData) => {
      if (error) return console.log(error)

      insertData(processedData, (error) => {
        if (error) plugin.logException(error)
      })
    })
  } else if (Array.isArray(data)) {
    async.each(data, function (datum) {
      processData(datum, (error, processedData) => {
        if (error) return console.log(error)
        insertData(processedData, (error) => {
          if (error) plugin.logException(error)
        })
      })
    })
  } else {
    plugin.logException(new Error(`Invalid data received. Data must be a valid Array/JSON Object or a collection of objects. Data: ${data}`))
  }
})

plugin.once('ready', () => {
  let options = plugin.config
  tableName = options.table

  async.waterfall([
    async.constant(options.field_mapping || '{}'),
    async.asyncify(JSON.parse),
    (obj, done) => {
      fieldMapping = obj
      done()
    }
  ], (parseError) => {
    if (parseError) {
      plugin.logException(new Error('Invalid field mapping. Must be a valid JSON String.'))

      return setTimeout(() => {
        process.exit(1)
      }, 5000)
    }

    let isEmpty = require('lodash.isempty')

    async.forEachOf(fieldMapping, (field, key, callback) => {
      if (isEmpty(field.source_field)) {
        callback(new Error(`Source field is missing for ${key} in field mapping.`))
      } else if (field.data_type && (field.data_type !== 'String' && field.data_type !== 'Integer' && field.data_type !== 'Float' && field.data_type !== 'Boolean' && field.data_type !== 'Date' && field.data_type !== 'DateTime' && field.data_type !== 'Timestamp')) {
        callback(new Error(`Invalid Data Type for ${key} in field mapping. Allowed data types are String, Integer, Float, Boolean, Date, DateTime and Timestamp.`))
      } else {
        callback()
      }
    }, (fieldMapError) => {
      if (fieldMapError) {
        console.error('Error parsing field mapping.', fieldMapError)
        plugin.logException(fieldMapError)

        return setTimeout(() => {
          process.exit(1)
        }, 5000)
      }

      let mysql = require('mysql')

      pool = mysql.createPool({
        host: options.host,
        port: options.port,
        user: options.user,
        password: options.password,
        database: options.database,
        acquireTimeout: 15000
      })

      pool.on('error', (mysqlError) => {
        plugin.logException(mysqlError)
      })

      plugin.log('MySQL Storage initialized.')
      plugin.emit('init')
    })
  })
})

module.exports = plugin
