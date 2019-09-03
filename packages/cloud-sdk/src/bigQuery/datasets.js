const consola = require('consola')
  .withDefaults({ badge: true })
  .withTag('bigQuery/datasets')

module.exports = bigquery => {
  async function exists(datasetId) {
    try {
      consola.log(`Check if exist dataset "${datasetId}"`)
      return await bigquery.dataset(datasetId).exists()
    } catch (err) {
      consola.error(err.message)
    }
  }

  async function get(datasetId) {
    try {
      consola.log(`Getting dataset "${datasetId}"`)
      return await bigquery.dataset(datasetId).get()
    } catch (err) {
      consola.error(err.message)
    }
  }

  async function create(datasetId, options = { location: 'EU' }) {
    try {
      consola.log(`Creating dataset "${datasetId}"`)
      return await bigquery.createDataset(datasetId, options)
    } catch (err) {
      consola.error(err.message)
    }
  }

  async function remove(datasetId, options = {}) {
    try {
      consola.log(`Removing dataset "${datasetId}"`)
      return await bigquery.dataset(datasetId).delete(options)
    } catch (err) {
      consola.error(err.message)
    }
  }

  return {
    exists,
    get,
    create,
    remove,
  }
}
