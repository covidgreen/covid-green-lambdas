const fetch = require('node-fetch')
const querystring = require('querystring')
const SQL = require('@nearform/sql')
const { getDatabase, getInteropConfig, runIfDev } = require('./utils')

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
  const query = SQL`INSERT INTO exposures (key_data, rolling_period, rolling_start_number, transmission_risk_level, regions) VALUES `

  for (const [
    index,
    {
      keyData,
      rollingPeriod,
      rollingStartNumber,
      transmissionRiskLevel,
      regions
    }
  ] of exposures.entries()) {
    query.append(
      SQL`(
        ${keyData},
        ${rollingPeriod},
        ${rollingStartNumber},
        ${transmissionRiskLevel},
        ${regions}
      )`
    )

    if (index < exposures.length - 1) {
      query.append(SQL`, `)
    }
  }

  query.append(
    SQL` ON CONFLICT ON CONSTRAINT exposures_key_data_unique DO NOTHING`
  )

  await client.query(query)
}

exports.handler = async function () {
  const { maxAge, token, url } = await getInteropConfig()
  const client = await getDatabase()
  const date = new Date()

  date.setDate(date.getDate() - maxAge)

  let more = true
  let batchTag = await getFirstBatchTag(client)

  do {
    const query = querystring.stringify({ batchTag })
    const downloadUrl = `${url}/download/${date.toISOString().substr(0, 10)}?${query}`

    const response = await fetch(downloadUrl, {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${token}`,
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
            throw new BadRequest('Invalid key length')
          }
        }

        await insertExposures(client, data.exposures)
      }

      await insertBatch(client, batchTag)

      console.log(`added ${data.exposures.length} exposures from batch ${batchTag}`)
    } else if (response.status === 204) {
      more = false

      console.log('no more batches to download')
    } else {
      throw new Exception('Request failed')
    }
  } while (more)
}

runIfDev(exports.handler)
