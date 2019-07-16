const { bigQuery } = require('@i62navpm/cloud-sdk')
const { specialtyCodes } = require('@i62navpm/specialty-codes')
const consola = require('consola')
  .withDefaults({ badge: true })
  .withTag('static-lists')

async function init(datasets = []) {
  for (const { dataset, tables, schema } of datasets) {
    consola.info(`Generating '${dataset}' dataset`)

    const [existDataset = false] = await bigQuery.datasets.exists(dataset)
    if (existDataset) {
      await bigQuery.datasets.remove(dataset, { force: true })
    }
    await bigQuery.datasets.create(dataset)
    consola.success(`Dataset '${dataset}' is generated`)

    consola.info(`Generating tables for '${dataset}' dataset`)
    await Promise.all(
      tables.map(async ({ id, description }) =>
        bigQuery.tables.create(dataset, id, { schema, description })
      )
    )
    consola.success(`Tables for '${dataset}' are generated`)
  }
}

init()
