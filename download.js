const fetch = require('node-fetch')
const querystring = require('querystring')
const SQL = require('@nearform/sql')
const {
  withDatabase,
  getInteropConfig,
  insertMetric,
  runIfDev
} = require('./utils')

async function getFirstBatchTag(client, serverId) {
  const query = SQL`
    SELECT batch_tag AS "batchTag"
    FROM download_batches
    WHERE server_id = ${serverId}
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

async function insertBatch(client, batchTag, serverId) {
  const query = SQL`
    INSERT INTO download_batches (batch_tag, server_id)
    VALUES (${batchTag}, ${serverId})
  `

  await client.query(query)
}

async function insertExposures(client, exposures) {
  const query = SQL`INSERT INTO exposures (key_data, rolling_period, rolling_start_number, transmission_risk_level, regions, origin) VALUES `

  for (const [
    index,
    {
      keyData,
      rollingPeriod,
      rollingStartNumber,
      transmissionRiskLevel,
      regions,
      origin
    }
  ] of exposures.entries()) {
    query.append(
      SQL`(
        ${keyData},
        ${rollingPeriod},
        ${rollingStartNumber},
        ${transmissionRiskLevel},
        ${regions},
        ${origin}
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
  const { servers } = await getInteropConfig()

  await withDatabase(async client => {
    for (const { id, maxAge, token, url } of servers) {
      const date = new Date()

      console.log(`beginning download from ${id}`)

      let more = true
      let batchTag = await getFirstBatchTag(client, id)
      let inserted = 0

      date.setDate(date.getDate() - maxAge)

      do {
        const query = querystring.stringify({ batchTag })
        const downloadUrl = `${url}/download/${date
          .toISOString()
          .substr(0, 10)}?${query}`

        const response = await fetch(downloadUrl, {
          method: 'GET',
          headers: {
            Authorization: `Bearer ${token}`,
            'Content-Type': 'application/json'
          }
        })

        if (response.status === 200) {
          const data = await response.json()

          batchTag = data.batchTag

          if (data.exposures.length > 0) {
            for (const { keyData } of data.exposures) {
              const decodedKeyData = Buffer.from(keyData, 'base64')

              if (decodedKeyData.length !== 16) {
                throw new Error('Invalid key length')
              }
            }

            inserted += await insertExposures(client, data.exposures)
          }

          await insertBatch(client, batchTag, id)

          console.log(
            `added ${data.exposures.length} exposures from batch ${batchTag}`
          )
        } else if (response.status === 204) {
          await insertMetric(
            client,
            'INTEROP_KEYS_DOWNLOADED',
            '',
            '',
            inserted
          )

          more = false
          console.log('no more batches to download')
        } else {
          throw new Error('Request failed')
        }
      } while (more)
    }
  })
}

runIfDev(exports.handler)
