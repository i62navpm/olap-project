const { BigQuery } = require('@google-cloud/bigquery')
const { credentials } = require('../config')
const bigQuery = new BigQuery(credentials)

exports.bigQuery = {
  datasets: require('./bigQuery/datasets')(bigQuery),
  tables: require('./bigQuery/tables')(bigQuery),
  queries: require('./bigQuery/queries')(bigQuery),
}
exports.bigQuery.queries
  .getDatesList({ datasetId: 'assignmentList', tableId: 'PT' })
  .then(data => {
    console.log(data)
  })
  .catch(err => {
    console.log(err)
  })
