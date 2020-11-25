const fetch = require('node-fetch')
const SQL = require('@nearform/sql')
const { JWK, JWS } = require('node-jose')
const {
  withDatabase,
  getInteropConfig,
  insertMetric,
  runIfDev
} = require('./utils')

async function createBatch(client, count, lastExposureId, serverId) {
  const query = SQL`
    INSERT INTO upload_batches (exposure_count, last_exposure_id, server_id)
    VALUES (${count}, ${lastExposureId}, ${serverId})
    RETURNING id
  `

  const { rows } = await client.query(query)
  const [{ id }] = rows

  return id
}

async function getFirstExposureId(client, serverId) {
  const query = SQL`
    SELECT COALESCE(MAX(last_exposure_id), 0) AS "firstExposureId"
    FROM upload_batches
    WHERE server_id = ${serverId}
  `

  const { rows } = await client.query(query)
  const [{ firstExposureId }] = rows

  return firstExposureId
}

async function getExposures(client, since) {
  const query = SQL`
    SELECT id, created_at, key_data, rolling_period, rolling_start_number, transmission_risk_level, regions
    FROM exposures
    WHERE id > ${since} AND origin IS NULL
    ORDER BY id ASC
  `

  const { rows } = await client.query(query)

  return rows
}

exports.handler = async function() {
  const { servers } = await getInteropConfig()

  await withDatabase(async client => {
    for (const { id, privateKey, token, url } of servers) {
      console.log(`beginning upload to ${id}`)

      const firstExposureId = await getFirstExposureId(client, id)
      const exposures = await getExposures(client, firstExposureId)

      if (exposures.length === 0) {
        console.log('no exposures to upload')
      } else {
        await client.query('BEGIN')

        try {
          const lastExposureId = exposures[exposures.length - 1].id

          const batchTag = await createBatch(
            client,
            exposures.length,
            lastExposureId,
            id
          )

          const key = await JWK.asKey(privateKey, 'pem')
          const sign = JWS.createSign({ format: 'compact' }, key)

          const payload = exposures.map(
            ({
              key_data: keyData,
              rolling_start_number: rollingStartNumber,
              transmission_risk_level: transmissionRiskLevel,
              rolling_period: rollingPeriod,
              regions
            }) => ({
              keyData,
              rollingStartNumber,
              transmissionRiskLevel,
              rollingPeriod,
              regions
            })
          )

          const result = await fetch(`${url}/upload`, {
            method: 'POST',
            headers: {
              Authorization: `Bearer ${token}`,
              'Content-Type': 'application/json'
            },
            body: JSON.stringify({
              batchTag,
              payload: await sign
                .update(JSON.stringify(payload), 'utf8')
                .final()
            })
          })

          if (!result.ok) {
            throw new Error(`Upload failed with ${result.status} response`)
          }

          const { insertedExposures } = await result.json()

          await insertMetric(
            client,
            'INTEROP_KEYS_UPLOADED',
            '',
            '',
            Number(insertedExposures)
          )

          await client.query('COMMIT')

          console.log(
            `uploaded ${exposures.length} to batch ${batchTag}, ${insertedExposures} of which were stored`
          )
        } catch (err) {
          await client.query('ROLLBACK')
          throw err
        }
      }
    }
  })
}

runIfDev(exports.handler)
