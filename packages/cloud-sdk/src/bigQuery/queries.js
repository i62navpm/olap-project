const consola = require('consola')
  .withDefaults({ badge: true })
  .withTag('bigQuery/queries')

module.exports = bigquery => {
  async function getPaginateList(
    datasetId,
    tableId,
    {
      paginate = { page: 0, limit: 10 },
      filter = { name: '' },
      sort = 'orden',
    } = {}
  ) {
    const query = `SELECT * FROM ${datasetId}.${tableId} 
    ${filter.name ? `WHERE apellidosynombre LIKE "${filter.name}%"` : ''} 
    ORDER BY ${sort} LIMIT @limit OFFSET @page`

    const options = {
      query,
      location: 'US',
      params: { limit: paginate.limit, page: paginate.page * paginate.limit },
    }
    try {
      consola.log('Running query')
      return await bigquery.query(options)
    } catch (err) {
      consola.error(err.message)
    }
  }

  async function getPosition(datasetId, { name = '', nif = '' } = {}) {
    const query = `
    WITH subQ1 as (SELECT orden, _TABLE_SUFFIX as tableId  FROM \`${datasetId}.*\`  WHERE apellidosynombre = @name and nif = @nif)

    SELECT count(apellidosynombre) as position, subQ1.tableId FROM subQ1, \`${datasetId}.*\` as datasetId WHERE datasetId.orden < subQ1.orden and datasetId._TABLE_SUFFIX = subQ1.tableId GROUP BY subQ1.tableId 
    `
    const options = {
      query,
      location: 'US',
      params: { name, nif },
    }
    try {
      consola.log('Running query')
      return await bigquery.query(options)
    } catch (err) {
      consola.error(err.message)
    }
  }

  async function getMovementsByDate(datasetId, { name = '', nif = '' } = {}) {
    const dateInitial = '2019-07-24'

    const query = `
    WITH subQ1 as (SELECT orden, _TABLE_SUFFIX as tableId  FROM \`${datasetId}.*\`  WHERE apellidosynombre = @name and nif = @nif),
         subQ2 as (SELECT nif, apellidosynombre, subQ1.tableId FROM subQ1, \`${datasetId}.*\` as datasetId WHERE datasetId.orden < subQ1.orden and datasetId._TABLE_SUFFIX = subQ1.tableId),
         subQ3 as (SELECT tableId, struct(date as date, -count(DISTINCT subQ2.apellidosynombre) as counter) as data FROM subQ2, \`assignmentList.*\` as assignmentList WHERE assignmentList.nif = subQ2.nif and assignmentList.apellidosynombre = subQ2.apellidosynombre and assignmentList.date BETWEEN @date and CURRENT_DATE() GROUP BY tableId, date )

    SELECT tableId, ARRAY_CONCAT_AGG([struct(@date as date, 0 as counter), data] order by data.date) as info FROM subQ3 GROUP BY tableId

    `
    const options = {
      query,
      location: 'US',
      params: { name, nif, date: dateInitial },
    }
    try {
      consola.log('Running query')
      return await bigquery.query(options)
    } catch (err) {
      consola.error(err.message)
    }
  }

  return {
    getPaginateList,
    getPosition,
    getMovementsByDate,
  }
}
