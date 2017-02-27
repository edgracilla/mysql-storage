/* global describe, it, before, after */
'use strict'

const should = require('should')
const moment = require('moment')
const amqp = require('amqplib')

const _ID = new Date().getTime()
const INPUT_PIPE = 'demo.pipe.storage'
const BROKER = 'amqp://guest:guest@127.0.0.1/'

let _app = null
let _conn = null
let _channel = null

let conf = {
  host: 'reekoh-mysql.cg1corueo9zh.us-east-1.rds.amazonaws.com',
  port: 3306,
  user: 'reekoh',
  password: 'rozzwalla',
  database: 'reekoh',
  table: 'reekoh_table',
  field_mapping: JSON.stringify({
    _id: {source_field: '_id', data_type: 'Integer'},
    co2_field: {source_field: 'co2', data_type: 'String'},
    temp_field: {source_field: 'temp', data_type: 'Integer'},
    quality_field: {source_field: 'quality', data_type: 'Float'},
    'Power(KW)': {source_field: 'power', data_type: 'Integer'},
    reading_time_field: {
      source_field: 'reading_time',
      data_type: 'Timestamp'
    },
    metadata_field: {source_field: 'metadata', data_type: 'String'},
    random_data_field: {source_field: 'random_data'},
    is_normal_field: {source_field: 'is_normal', data_type: 'Boolean'}
  })
}

let record = {
  _id: _ID,
  co2: '11%',
  temp: 23,
  quality: 11.25,
  power: 90,
  reading_time: '2015-11-27T11:04:13.000Z',
  metadata: '{"metadata_json": "reekoh metadata json"}',
  random_data: 'abcdefg',
  is_normal: true
}

describe('MySQL Storage', function () {
  before('init', () => {
    process.env.BROKER = BROKER
    process.env.INPUT_PIPE = INPUT_PIPE
    process.env.CONFIG = JSON.stringify(conf)

    amqp.connect(BROKER).then((conn) => {
      _conn = conn
      return conn.createChannel()
    }).then((channel) => {
      _channel = channel
    }).catch((err) => {
      console.log(err)
    })
  })

  after('terminate', function () {
    _conn.close()
  })

  describe('#start', function () {
    it('should start the app', function (done) {
      this.timeout(10000)
      _app = require('../app')
      _app.once('init', done)
    })
  })

  describe('#data', function () {
    it('should process the data', function (done) {
      this.timeout(5000)
      _channel.sendToQueue(INPUT_PIPE, new Buffer(JSON.stringify(record)))
      _app.on('processed', done)
    })
  })

  describe('#data', function () {
    it('should have inserted the data', function (done) {
      this.timeout(10000)

      let mysql = require('mysql')

      let connection = mysql.createConnection({
        host: conf.host,
        port: conf.port,
        user: conf.user,
        password: conf.password,
        database: conf.database
      })

      connection.connect(function (err) {
        should.ifError(err)

        connection.query('SELECT * FROM ' + conf.table + ' WHERE _id = ' + _ID, function (error, result, fields) {
          should.ifError(error)
          should.exist(result[0])
          let resp = result[0]

          // cleanup for JSON stored string
          let cleanMetadata = resp.metadata_field.replace(/\\"/g, '"')
          let str = JSON.stringify(record.metadata)
          let str2 = JSON.stringify(cleanMetadata)

          should.equal(record.co2, resp.co2_field, 'Data validation failed. Field: co2')
          should.equal(record.temp, resp.temp_field, 'Data validation failed. Field: temp')
          should.equal(record.quality, resp.quality_field, 'Data validation failed. Field: quality')
          should.equal(record.random_data, resp.random_data_field, 'Data validation failed. Field: random_data')
          should.equal(moment(record.reading_time).format('YYYY-MM-DD HH:mm:ss'), moment(resp.reading_time_field).format('YYYY-MM-DD HH:mm:ss'), 'Data validation failed. Field: reading_time')
          should.equal(str, str2, 'Data validation failed. Field: metadata')

          done()
        })
      })
    })
  })
})
