const { bigQuery } = require('@i62navpm/cloud-sdk')
const { specialtyCodes } = require('@i62navpm/specialty-codes')
const consola = require('consola')
  .withDefaults({ badge: true })
  .withTag('lists-dynamic')

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

    const options = {
      timePartitioning: {
        type: 'DAY',
        field: 'date',
      },
      requirePartitionFilter: true,
      clustering: { fields: ['nif', 'apellidosynombre'] },
    }

    await Promise.all(
      tables.map(async ({ id, description }) =>
        bigQuery.tables.create(dataset, id, { schema, description, ...options })
      )
    )
    consola.success(`Tables for '${dataset}' are generated`)
  }
}

init([
  {
    dataset: 'assignmentList',
    tables: [
      ...specialtyCodes.primary.bilingualSpecialties,
      ...specialtyCodes.primary.normalSpecialties,
    ],
    schema: require('../schemas/assignmentList'),
  },
  {
    dataset: 'citationList',
    tables: [
      ...specialtyCodes.primary.bilingualSpecialties,
      ...specialtyCodes.primary.normalSpecialties,
    ],
    schema: require('../schemas/citationList'),
  },
  {
    dataset: 'citationVoluntaryList',
    tables: specialtyCodes.primary.normalSpecialties,
    schema: require('../schemas/citationVoluntaryList'),
  },
  {
    dataset: 'incorporateList',
    tables: [
      ...specialtyCodes.primary.bilingualSpecialties,
      ...specialtyCodes.primary.normalSpecialties,
    ],
    schema: require('../schemas/incorporateList'),
  },
  {
    dataset: 'nextCitationList',
    tables: [
      ...specialtyCodes.primary.bilingualSpecialties,
      ...specialtyCodes.primary.normalSpecialties,
    ],
    schema: require('../schemas/nextCitationList'),
  },
])
