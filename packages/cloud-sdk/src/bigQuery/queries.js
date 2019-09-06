const consola = require('consola')
  .withDefaults({ badge: true })
  .withTag('bigQuery/queries')

module.exports = bigquery => {
  async function existsDateColumn(data = {}) {
    const { datasetId, tableId } = data

    const query = `SELECT data_type
    FROM ${datasetId}.INFORMATION_SCHEMA.COLUMNS
    where table_name="${tableId}" and data_type = "DATE"`

    const options = {
      query,
    }

    try {
      consola.log('Running query')
      return await bigquery.query(options)
    } catch (err) {
      consola.error(err.message)
    }
  }

  async function getDatesList(data = {}) {
    const { datasetId, tableId } = data

    const query = `SELECT DISTINCT(date) FROM ${datasetId}.${tableId} WHERE date <= CURRENT_DATE()`

    const options = {
      query,
    }

    try {
      consola.log('Running query')
      const [dates] = await existsDateColumn(data)
      return dates.length ? await bigquery.query(options) : Promise.resolve([])
    } catch (err) {
      consola.error(err.message)
    }
  }

  async function getPaginateList(
    datasetId,
    tableId,
    {
      paginate = { page: 0, limit: 10 },
      filter = { name: '', date: '' },
      sort = 'orden',
      order = 'ASC',
    } = {}
  ) {
    const query = `with subQ1 as (SELECT * FROM ${datasetId}.${tableId} 
    WHERE apellidosynombre LIKE "${filter.name}%" ${
      filter.date ? `AND date = "${filter.date}"` : ''
    } 
    ORDER BY ${sort} ${order} LIMIT @limit OFFSET @page)
    
    select count(*) as total, array_agg(subQ1) as data from subQ1
    `

    const options = {
      query,
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
    WITH subQ1 as (select orden, _TABLE_SUFFIX as tableId  from \`${datasetId}.*\`  where apellidosynombre = @name and nif = @nif),
         subQ2 as (select nif, apellidosynombre, subQ1.tableId from subQ1, \`${datasetId}.*\` as normalList where normalList.orden < subQ1.orden and normalList._TABLE_SUFFIX = subQ1.tableId),
         subQ3 as (select tableId, date, -count(DISTINCT subQ2.apellidosynombre) as counter from subQ2, \`assignmentList.*\` as assignmentList where assignmentList.nif = subQ2.nif and assignmentList.apellidosynombre = subQ2.apellidosynombre and assignmentList.date BETWEEN @date and CURRENT_DATE() group by tableId, date ),
         subQ4 as (select tableId, Date(@date) as date, count(subQ2.apellidosynombre) as counter from subQ2 group by tableId),
         subQ5 as (select tableId, struct(date, counter, SUM(counter) OVER (PARTITION BY tableId ORDER BY date) as acc) as data from (select date, tableId, counter from subQ3 UNION ALL select date, tableId, counter from subQ4) as unionQuery)
   
    select tableId, ARRAY_CONCAT_AGG([data] order by data.date) as info from subQ5 group by tableId
    `
    const options = {
      query,
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
    getDatesList,
  }
}
