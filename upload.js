const fetch = require('node-fetch')
const SQL = require('@nearform/sql')
const { JWK, JWS } = require('node-jose')
const { getDatabase, getInteropConfig, runIfDev } = require('./utils')

async function createBatch(client, count, lastExposureId) {
  const query = SQL`
    INSERT INTO upload_batches (exposure_count, last_exposure_id)
    VALUES (${count}, ${lastExposureId})
    RETURNING id
  `

  const { rows } = await client.query(query)
  const [{ id }] = rows

  return id
}

async function getFirstExposureId(client) {
  const query = SQL`
    SELECT COALESCE(MAX(last_exposure_id), 0) AS "firstExposureId"
    FROM upload_batches
  `

  const { rows } = await client.query(query)
  const [{ firstExposureId }] = rows

  return firstExposureId
}

async function getExposures(client, since) {
  const query = SQL`
    SELECT id, created_at, key_data, rolling_period, rolling_start_number, transmission_risk_level, regions
    FROM exposures
    WHERE id > ${since}
    ORDER BY id ASC
  `

  const { rows } = await client.query(query)

  return rows
}

exports.handler = async function () {
  const { privateKey, token, url } = await getInteropConfig()
  const client = await getDatabase()
  const firstExposureId = await getFirstExposureId(client)
  const exposures = await getExposures(client, firstExposureId)

  if (exposures.length === 0) {
    console.log('no exposures to upload')
  } else {
    await client.transact(async transaction => {
      const lastExposureId = exposures[exposures.length - 1].id
      const batchTag = await createBatch(transaction, exposures.length, lastExposureId)
      const key = await JWK.asKey(privateKey, 'pem')
      const sign = JWS.createSign({ format: 'compact' }, key)

      const payload = exposures.map(({ key_data, rolling_start_number, transmission_risk_level, rolling_period, regions }) => ({
        keyData: key_data,
        rollingStartNumber: rolling_start_number,
        transmissionRiskLevel: transmission_risk_level,
        rollingPeriod: rolling_period,
        regions: regions
      }))

      const result = await fetch(`${url}/upload`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          batchTag,
          payload: await sign.update(JSON.stringify(payload), 'utf8').final()
        })
      })

      if (!result.ok) {
        throw new Error(`Upload failed with ${result.status} response`)
      }

      const { insertedExposures } = await result.json()

      console.log(`uploaded ${exposures.length} to batch ${batchTag}, ${insertedExposures} of which were stored`)
    })
  }
}

runIfDev(exports.handler)
