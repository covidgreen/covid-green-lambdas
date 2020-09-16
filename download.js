const fetch = require('node-fetch')
const SQL = require('@nearform/sql')
const {
  getDatabase,
  getInteropConfig,
  insertMetric,
  runIfDev
} = require('./utils')

async function getFirstBatchTag(client) {
  const query = SQL`
    SELECT batch_tag AS "batchTag"
    FROM download_batches
    ORDER BY created_at DESC
    LIMIT 1
  `

  const { rowCount, rows } = await client.query(query)

  if (rowCount === 0) {
    return ''
  }

  const [{ batchTag }] = rows

  return batchTag
}

async function insertBatch(client, batchTag) {
  const query = SQL`
    INSERT INTO download_batches (batch_tag)
    VALUES (${batchTag})
  `

  await client.query(query)
}

async function insertExposures(client, exposures) {
  const query = SQL`INSERT INTO exposures (key_data, rolling_period, rolling_start_number, transmission_risk_level, regions, origin, days_since_onset) VALUES `

  for (const [
    index,
    {
      keyData,
      rollingPeriod,
      rollingStartIntervalNumber,
      transmissionRiskLevel,
      visitedCountries,
      origin,
      days_since_onset_of_symptoms: daysSinceOnset // eslint-disable-line camelcase
    }
  ] of exposures.entries()) {
    query.append(
      SQL`(
        ${keyData},
        ${rollingPeriod},
        ${rollingStartIntervalNumber},
        ${transmissionRiskLevel},
        ${visitedCountries},
        ${origin},
        ${daysSinceOnset} 
      )`
    )

    if (index < exposures.length - 1) {
      query.append(SQL`, `)
    }
  }

  query.append(
    SQL` ON CONFLICT ON CONSTRAINT exposures_key_data_unique DO NOTHING`
  )

  const { rowCount } = await client.query(query)

  return rowCount
}

exports.handler = async function() {
  const { maxAge, token, url } = await getInteropConfig()
  const client = await getDatabase()
  const date = new Date()

  date.setDate(date.getDate() - maxAge)

  let more = true
  let batchTag = await getFirstBatchTag(client)
  let inserted = 0

  do {
    const downloadUrl = `${url}/download/${date.toISOString().substr(0, 10)}`

    const response = await fetch(downloadUrl, {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/json',
        batchTag
      }
    })

    if (response.status === 200) {
      const data = await response.json()

      if (data.keys.length > 0) {
        for (const { keyData } of data.keys) {
          const decodedKeyData = Buffer.from(keyData, 'base64')

          if (decodedKeyData.length !== 16) {
            throw new Error('Invalid key length')
          }
        }

        inserted += await insertExposures(client, data.keys)
      }

      await insertBatch(client, data.batchTag)

      console.log(
        `added ${data.keys.length} exposures from batch ${data.batchTag}`
      )

      if (data.nextBatchTag) {
        batchTag = data.nextBatchTag
      } else {
        more = false
      }
    } else if (response.status === 204) {
      await insertMetric(client, 'INTEROP_KEYS_DOWNLOADED', '', '', inserted)

      more = false
      console.log('no more batches to download')
    } else {
      throw new Error('Request failed')
    }
  } while (more)
}

runIfDev(exports.handler)
