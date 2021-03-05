const axios = require('axios')
const fetch = require('node-fetch')
const https = require('https')
const querystring = require('querystring')
const SQL = require('@nearform/sql')
const {
  getInteropConfig,
  insertMetric,
  runIfDev,
  withDatabase,
  getTimeZone
} = require('./utils')

async function getFirstBatchTag(client, serverId, date) {
  const query = SQL`
    SELECT batch_tag AS "batchTag"
    FROM download_batches
    WHERE server_id = ${serverId}`

  if (date) {
    query.append(SQL`
      AND created_at::DATE = ${date}::DATE
    `)
  }

  query.append(SQL`
    ORDER BY created_at DESC
    LIMIT 1
  `)

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
        ${transmissionRiskLevel || 0},
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

async function downloadFromInterop(
  client,
  id,
  maxAge,
  token,
  url,
  event,
  allowedTestTypes
) {
  console.log(`beginning download from ${url}`)

  let more = true
  let batchTag = await getFirstBatchTag(client, id)
  let inserted = 0

  const date = event.date ? new Date(event.date) : new Date()
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
      let actualKeys = 0
      batchTag = data.batchTag

      if (data.exposures.length > 0) {
        for (const { keyData } of data.exposures) {
          const decodedKeyData = Buffer.from(keyData, 'base64')

          if (decodedKeyData.length !== 16) {
            throw new Error('Invalid key length')
          }
        }

        // filter keys based on allowed test type criteria and report type
        const validKeys = data.exposures.filter((exp) => {
          return (
            (allowedTestTypes.length === 0 ||
              allowedTestTypes.indexOf(exp.testType) > -1) &&
            exp.reportType === 1
          )
        })
        if (validKeys.length > 0) {
          actualKeys = await insertExposures(client, validKeys)
          inserted += actualKeys
        }
      }

      await insertBatch(client, batchTag, id)

      console.log(
        `added ${actualKeys} exposures from potential ${data.exposures.length} exposures from batch ${batchTag}`
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

async function updateMetrics(client, interopOrigin) {
  const timeZone = await getTimeZone()
  const query = SQL`INSERT INTO metrics (event, date, value, os, version) 
      SELECT CONCAT('INTEROP_KEYS_DOWNLOADED_', COALESCE(origin, ${interopOrigin})),      
      (CURRENT_TIMESTAMP AT TIME ZONE ${timeZone})::DATE,
      COUNT(*), '', '' FROM exposures 
      WHERE created_at >= (CURRENT_TIMESTAMP AT TIME ZONE ${timeZone})::DATE 
      AND created_at < (CURRENT_TIMESTAMP AT TIME ZONE ${timeZone})::DATE + 1
      GROUP BY origin
      ON CONFLICT ON CONSTRAINT metrics_pkey
      DO UPDATE SET value = EXCLUDED.value 
      WHERE metrics.date = EXCLUDED.date AND metrics.event = EXCLUDED.event
  `
  await client.query(query)
}

async function downloadFromEfgs(client, config, event, interopOrigin) {
  const { auth, url } = config
  const date = event.date ? new Date(event.date) : new Date()

  console.log(`beginning download from ${url}`)

  const httpsAgent = new https.Agent({
    cert: Buffer.from(auth.cert, 'utf-8'),
    key: Buffer.from(auth.key, 'utf-8')
  })

  let batchTag = await getFirstBatchTag(client, 'efgs', date)
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
        `${url}/diagnosiskeys/download/${date.toISOString().substr(0, 10)}`,
        {
          headers,
          httpsAgent
        }
      )

      if (result.data.keys) {
        const keys = []

        for (const {
          keyData,
          rollingStartIntervalNumber,
          rollingPeriod,
          transmissionRiskLevel,
          origin,
          reportType,
          days_since_onset_of_symptoms: daysSinceOnsetOfSymptoms
        } of result.data.keys) {
          if (
            reportType === 'CONFIRMED_TEST' &&
            Buffer.from(keyData, 'base64').length === 16
          ) {
            keys.push({
              keyData,
              rollingPeriod,
              rollingStartNumber: rollingStartIntervalNumber,
              transmissionRiskLevel,
              regions: [origin],
              origin,
              daysSinceOnset: daysSinceOnsetOfSymptoms
            })
          }
        }

        await insertBatch(client, result.headers.batchtag, 'efgs')

        if (keys.length > 0) {
          const inserted = await insertExposures(client, keys)
          await insertMetric(
            client,
            'INTEROP_KEYS_DOWNLOADED',
            '',
            '',
            inserted
          )
        }

        console.log(
          `inserted ${keys.length} keys from batch ${result.headers.batchtag}`
        )
      } else {
        console.log(`batch ${batchTag} contained no keys, skipping`)
      }

      if (
        result.headers.nextbatchtag &&
        result.headers.nextbatchtag !== 'null'
      ) {
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
  await updateMetrics(client, interopOrigin)
}

exports.handler = async function (event) {
  const { efgs, servers, origin, allowedTestTypes } = await getInteropConfig()

  await withDatabase(async (client) => {
    for (const { id, maxAge, token, url } of servers) {
      await downloadFromInterop(
        client,
        id,
        maxAge,
        token,
        url,
        event,
        allowedTestTypes
      )
    }

    if (efgs && efgs.download) {
      await downloadFromEfgs(client, efgs, event, origin)
    }
  })
}

runIfDev(exports.handler)
