const { bigQuery } = require('@i62navpm/cloud-sdk')
const { getCurrentDate } = require('./helpers/date')
const path = require('path')
const fs = require('fs')
const consola = require('consola')
  .withDefaults({ badge: true })
  .withTag('lists-static')

async function init(datasets = []) {
  for (const { dataset, date = getCurrentDate() } of datasets) {
    consola.info(`Looking for files for ['${dataset}'] dataset`)
    const folderPath = path.resolve('./docs', 'json', date, dataset)
    const files = fs.readdirSync(folderPath)

    consola.info(`Loading data for tables in ['${dataset}'] dataset`)
    for (const file of files) {
      const filename = path.resolve(folderPath, file)
      const table = path.basename(filename, '.json')
      await bigQuery.tables.load(dataset, table, filename)
    }
    consola.success(`Tables for ['${dataset}'] are loaded`)
  }
}

init([
  // {
  //   dataset: 'normalList',
  //   date: '2019-07-25',
  // },
  // {
  //   dataset: 'bilingualList',
  //   date: '2019-07-25',
  // },
  // {
  //   dataset: 'voluntaryList',
  //   date: '2019-08-01',
  // },
])
