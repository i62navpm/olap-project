const { BigQuery } = require('@google-cloud/bigquery')
const { credentials } = require('../../config')
const consola = require('consola')
  .withDefaults({ badge: true })
  .withTag('bigQuery')

const bigquery = new BigQuery(credentials)

async function createTable(datasetId, tableId, schema = []) {
  const options = {
    schema,
  }

  try {
    consola.info(`Creating table "${tableId}" in dataset "${datasetId}"`)
    return await bigquery.dataset(datasetId).createTable(tableId, options)
  } catch (err) {
    consola.error(err)
  }
}

async function deleteTable(datasetId, tableId) {
  try {
    consola.info(`Deleting table "${tableId}" in dataset "${datasetId}"`)
    return await bigquery
      .dataset(datasetId)
      .table(tableId)
      .delete()
  } catch (err) {
    consola.error(err)
  }
}

async function loadTable(datasetId, tableId, source) {
  const metadata = {
    encoding: 'ISO-8859-1',
    sourceFormat: 'NEWLINE_DELIMITED_JSON',
  }

  try {
    consola.info(`Loading data in table "${tableId}" in dataset "${datasetId}"`)
    return await bigquery
      .dataset(datasetId)
      .table(tableId)
      .load(source, metadata)
  } catch (err) {
    consola.error(err)
  }
}

module.exports = {
  createTable,
  deleteTable,
  loadTable,
}
