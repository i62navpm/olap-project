const { BigQuery } = require('@google-cloud/bigquery')
const { credentials } = require('../config')
const bigQuery = new BigQuery(credentials)

exports.bigQuery = {
  datasets: require('./bigQuery/datasets')(bigQuery),
  tables: require('./bigQuery/tables')(bigQuery),
}
