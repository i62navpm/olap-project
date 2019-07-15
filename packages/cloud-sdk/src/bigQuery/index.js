const { BigQuery } = require('@google-cloud/bigquery')
const { credentials } = require('../../config')

const bigquery = new BigQuery(credentials)

async function createTable(datasetId, tableId, schema = []) {
  const options = {
    schema,
  }

  try {
    return await bigquery.dataset(datasetId).createTable(tableId, options)
  } catch (err) {
    console.error(err)
  }
}

async function deleteTable(datasetId, tableId) {
  try {
    return await bigquery
      .dataset(datasetId)
      .table(tableId)
      .delete()
  } catch (err) {
    console.error(err)
  }
}

async function loadTable(datasetId, tableId, source) {
  const metadata = {
    encoding: 'ISO-8859-1',
    sourceFormat: 'NEWLINE_DELIMITED_JSON',
  }

  try {
    return await bigquery
      .dataset(datasetId)
      .table(tableId)
      .load(source, metadata)
  } catch (err) {
    console.error(err)
  }
}

module.exports = {
  createTable,
  deleteTable,
  loadTable,
}
