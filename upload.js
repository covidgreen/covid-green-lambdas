const fetch = require('node-fetch')
const jsrsasign = require('jsrsasign')
const SQL = require('@nearform/sql')
const {
  getDatabase,
  getInteropConfig,
  insertMetric,
  runIfDev
} = require('./utils')

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
    SELECT id, created_at, key_data, rolling_period, rolling_start_number, transmission_risk_level, regions, days_since_onset
    FROM exposures
    WHERE id > ${since} AND origin IS NULL
    ORDER BY id ASC
  `

  const { rows } = await client.query(query)

  return rows
}

exports.handler = async function() {
  const {
    certificate,
    origin,
    privateKey,
    token,
    url
  } = await getInteropConfig()
  const client = await getDatabase()
  const firstExposureId = await getFirstExposureId(client)
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
        lastExposureId
      )

      const reportTypes = {
        CONFIRMED_TEST: 1,
        CONFIRMED_CLINICAL_DIAGNOSIS: 2,
        SELF_REPORT: 3,
        RECURSIVE: 4,
        REVOKED: 5
      }

      const keys = exposures.map(
        ({
          key_data: keyData,
          rolling_start_number: rollingStartIntervalNumber,
          transmission_risk_level: transmissionRiskLevel,
          rolling_period: rollingPeriod,
          regions: visitedCountries,
          days_since_onset: days_since_onset_of_symptoms // eslint-disable-line camelcase
        }) => ({
          keyData,
          rollingStartIntervalNumber,
          transmissionRiskLevel,
          rollingPeriod,
          visitedCountries,
          reportType: 'CONFIRMED_CLINICAL_DIAGNOSIS',
          days_since_onset_of_symptoms, // eslint-disable-line camelcase
          origin
        })
      )

      const data = Buffer.concat(
        keys
          .sort((a, b) => {
            if (a.keyData < b.keyData) {
              return -1
            }

            if (a.keyData > b.keyData) {
              return 1
            }

            return 0
          })
          .map(
            ({
              keyData,
              rollingStartIntervalNumber,
              rollingPeriod,
              transmissionRiskLevel,
              visitedCountries,
              origin,
              reportType,
              days_since_onset_of_symptoms // eslint-disable-line camelcase
            }) => {
              const rollingStartIntervalNumberBuffer = Buffer.alloc(4)
              const rollingPeriodBuffer = Buffer.alloc(4)
              const transmissionRiskLevelBuffer = Buffer.alloc(4)
              const reportTypeBuffer = Buffer.alloc(4)
              const daysSinceOnsetOfSymptomsBuffer = Buffer.alloc(4)

              rollingStartIntervalNumberBuffer.writeUInt32BE(
                rollingStartIntervalNumber
              )
              rollingPeriodBuffer.writeUInt32BE(rollingPeriod)
              transmissionRiskLevelBuffer.writeInt32BE(transmissionRiskLevel)
              reportTypeBuffer.writeInt32BE(reportTypes[reportType] || 0)
              daysSinceOnsetOfSymptomsBuffer.writeUInt32BE(
                days_since_onset_of_symptoms
              )

              return Buffer.concat([
                Buffer.from(Buffer.from(keyData, 'base64').toString('utf-8')),
                rollingStartIntervalNumberBuffer,
                rollingPeriodBuffer,
                transmissionRiskLevelBuffer,
                ...visitedCountries.map(country => Buffer.from(country)),
                Buffer.from(origin),
                reportTypeBuffer,
                daysSinceOnsetOfSymptomsBuffer
              ])
            }
          )
      )

      const signed = jsrsasign.KJUR.asn1.cms.CMSUtil.newSignedData({
        content: { hex: data.toString('hex') },
        certs: [certificate],
        detached: false,
        signerInfos: [
          {
            hashAlg: 'sha256',
            sAttr: {
              SigningTime: {}
            },
            signerCert: certificate,
            sigAlg: 'SHA1withRSA',
            signerPrvKey: privateKey
          }
        ]
      })

      const result = await fetch(`${url}/upload`, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${token}`,
          batchSignature: Buffer.from(
            signed.getContentInfoEncodedHex(),
            'hex'
          ).toString('base64'),
          batchTag,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ keys })
      })

      if (!result.ok) {
        throw new Error(`Upload failed with ${result.status} response`)
      }

      await insertMetric(
        client,
        'INTEROP_KEYS_UPLOADED',
        '',
        '',
        Number(exposures.length)
      )

      await client.query('COMMIT')

      console.log(`uploaded ${exposures.length} to batch ${batchTag}`)
    } catch (err) {
      await client.query('ROLLBACK')
      throw err
    }
  }
}

runIfDev(exports.handler)
