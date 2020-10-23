const axios = require('axios')
const fetch = require('node-fetch')
const https = require('https')
const querystring = require('querystring')
const SQL = require('@nearform/sql')
const {
  getDatabase,
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
  const query = SQL`INSERT INTO exposures (key_data, rolling_period, rolling_start_number, transmission_risk_level, regions, origin, days_since_onset) VALUES `

  for (const [
    index,
    {
      keyData,
      rollingPeriod,
      rollingStartNumber,
      transmissionRiskLevel,
      regions,
      origin,
      daysSinceOnset
    }
  ] of exposures.entries()) {
    query.append(
      SQL`(
        ${keyData},
        ${rollingPeriod},
        ${rollingStartNumber},
        ${transmissionRiskLevel},
        ${regions},
        ${origin},
        ${daysSinceOnset || 0}
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

async function downloadFromInterop(client, id, maxAge, token, url) {
  const date = new Date()

  console.log(`beginning download from ${url}`)

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
      await insertMetric(client, 'INTEROP_KEYS_DOWNLOADED', '', '', inserted)

      more = false
      console.log('no more batches to download')
    } else {
      throw new Error('Request failed')
    }
  } while (more)
}

async function downloadFromEfgs(client, config) {
  const { auth, url } = config

  console.log(`beginning download from ${url}`)

  const httpsAgent = new https.Agent({
    cert: Buffer.from(auth.cert, 'utf-8'),
    key: Buffer.from(auth.key, 'utf-8')
  })

  const date = new Date().toISOString().substr(0, 10)

  let batchTag = null
  let more = true

  while (more) {
    const headers = {
      Accept: 'application/json; version=1.0'
    }

    if (batchTag) {
      headers.batchTag = batchTag
    }

    try {
      const result = await axios.get(
        `${url}/diagnosiskeys/download/${date}`,
        {
          headers,
          httpsAgent
        }
      )

      if (result.data.keys) {
        const keys = []

        for (const { keyData, rollingStartIntervalNumber, rollingPeriod, transmissionRiskLevel, origin, reportType, days_since_onset_of_symptoms } of result.data.keys) {
          if (reportType === 'CONFIRMED_TEST' && Buffer.from(keyData, 'base64').length === 16) {
            keys.push({
              keyData,
              rollingPeriod,
              rollingStartNumber: rollingStartIntervalNumber,
              transmissionRiskLevel,
              regions: [origin],
              origin,
              daysSinceOnset: days_since_onset_of_symptoms
            })
          }
        }

        if (keys.length > 0) {
          await insertExposures(client, keys)
        }

        console.log(`inserted ${keys.length} keys from batch ${batchTag}`)
      } else {
        console.log(`batch ${batchTag} contained no keys, skipping`)
      }

      if (result.headers.nextbatchtag && result.headers.nextbatchtag !== 'null') {
        batchTag = result.headers.nextbatchtag
      } else {
        more = false
      }
    } catch (err) {
      if (err.response && err.response.status && err.response.status === 404) {
        console.log(`no batches found to download`)
        more = false
      } else {
        throw err
      }
    }
  }
}

exports.handler = async function() {
  const { efgs, servers } = await getInteropConfig()
  const client = await getDatabase()

  for (const { id, maxAge, token, url } of servers) {
    await downloadFromInterop(client, id, maxAge, token, url)
  }

  if (efgs && efgs.download) {
    await downloadFromEfgs(client, efgs)
  }
}

runIfDev(exports.handler)
