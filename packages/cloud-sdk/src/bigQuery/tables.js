const consola = require('consola')
  .withDefaults({ badge: true })
  .withTag('bigQuery/tables')

module.exports = bigquery => {
  async function exists(datasetId, tableId) {
    try {
      consola.log(`Check if in "${datasetId}" exists table "${tableId}"`)
      return await bigquery
        .dataset(datasetId)
        .table(tableId)
        .exists()
    } catch (err) {
      consola.error(err.message)
    }
  }

  async function get(datasetId, tableId) {
    try {
      consola.log(`Getting table "${tableId}" in dataset "${datasetId}"`)
      return await bigquery.dataset(datasetId).table(tableId).get
    } catch (err) {
      consola.error(err.message)
    }
  }
  async function create(datasetId, tableId, options = {}) {
    try {
      consola.log(`Creating table "${tableId}" in dataset "${datasetId}"`)
      return await bigquery.dataset(datasetId).createTable(tableId, options)
    } catch (err) {
      consola.error(err.message)
    }
  }

  async function remove(datasetId, tableId) {
    try {
      consola.log(`Removing table "${tableId}" in dataset "${datasetId}"`)
      return await bigquery
        .dataset(datasetId)
        .table(tableId)
        .delete()
    } catch (err) {
      consola.error(err.message)
    }
  }

  async function load(datasetId, tableId, source) {
    const metadata = {
      encoding: 'ISO-8859-1',
      sourceFormat: 'NEWLINE_DELIMITED_JSON',
    }

    try {
      consola.log(
        `Loading data in table "${tableId}" in dataset "${datasetId}"`
      )
      return await bigquery
        .dataset(datasetId)
        .table(tableId)
        .load(source, metadata)
    } catch (err) {
      consola.error(err.message)
    }
  }

  return {
    exists,
    get,
    create,
    remove,
    load,
  }
}
