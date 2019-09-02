const consola = require('consola')
  .withDefaults({ badge: true })
  .withTag('bigQuery/queries')

module.exports = bigquery => {
  async function paginate(
    datasetId,
    tableId,
    { filter = { page: 0, limit: 10 }, sort = 'orden' } = {}
  ) {
    const query = `SELECT * FROM ${datasetId}.${tableId} ORDER BY ${sort} LIMIT @limit OFFSET @page`
    const options = {
      query,
      location: 'US',
      params: { limit: filter.limit, page: filter.page * filter.limit },
    }
    try {
      consola.log('Running query')
      return await bigquery.query(options)
    } catch (err) {
      consola.error(err.message)
    }
  }

  return {
    paginate,
  }
}
