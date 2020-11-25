const axios = require('axios')
const fetch = require('node-fetch')
const https = require('https')
const jsrsasign = require('jsrsasign')
const SQL = require('@nearform/sql')
const { JWK, JWS } = require('node-jose')
const { differenceInDays } = require('date-fns')
const {
  getInteropConfig,
  insertMetric,
  runIfDev,
  withDatabase
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
    SELECT id, created_at, key_data, rolling_period, rolling_start_number, transmission_risk_level, regions, days_since_onset
    FROM exposures
    WHERE id > ${since} AND origin IS NULL
    ORDER BY id ASC
    LIMIT 1000
  `

  const { rows } = await client.query(query)

  return rows
}

async function uploadToInterop(client, id, privateKey, token, url) {
  console.log(`beginning upload to ${url}`)

  let more = true

  while (more) {
    const firstExposureId = await getFirstExposureId(client, id)
    const exposures = await getExposures(client, firstExposureId)

    if (exposures.length === 0) {
      console.log('no more exposures to upload')
      more = false
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
            payload: await sign.update(JSON.stringify(payload), 'utf8').final()
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
}

async function uploadToEfgs(client, config) {
  const { auth, sign, url } = config

  console.log(`beginning upload to ${url}`)

  let more = true

  while (more) {
    const firstExposureId = await getFirstExposureId(client, 'efgs')
    const exposures = await getExposures(client, firstExposureId)
    const keysToUpload = []

    for (const { days_since_onset, key_data, rolling_period, rolling_start_number, transmission_risk_level } of exposures) {
      if (differenceInDays(new Date(), new Date(rolling_start_number * 1000 * 600)) < 14) {
        keysToUpload.push({
          keyData: key_data,
          rollingStartIntervalNumber: rolling_start_number,
          rollingPeriod: rolling_period,
          transmissionRiskLevel: transmission_risk_level,
          visitedCountries: ['AT', 'BE', 'BG', 'HR', 'CY', 'CZ', 'DK', 'EE', 'FI', 'FR', 'DE', 'GR', 'HU', 'IE', 'IT', 'LV', 'LT', 'LU', 'MT', 'NL', 'PL', 'PT', 'RO', 'SK', 'SI', 'ES', 'SE'],
          origin: 'IE',
          reportType: 'CONFIRMED_TEST',
          days_since_onset_of_symptoms: Math.min(Math.max(days_since_onset, 0), 14)
        })
      }
    }

    if (keysToUpload.length === 0) {
      console.log('no more exposures to upload')
      more = false
    } else {
      await client.query('BEGIN')

      try {
        const lastExposureId = exposures[exposures.length - 1].id

        const batchTag = await createBatch(
          client,
          exposures.length,
          lastExposureId,
          'efgs'
        )

        const httpsAgent = new https.Agent({
          cert: Buffer.from(auth.cert, 'utf-8'),
          key: Buffer.from(auth.key, 'utf-8')
        })

        const reportTypes = {
          CONFIRMED_TEST: 1,
          CONFIRMED_CLINICAL_DIAGNOSIS: 2,
          SELF_REPORT: 3,
          RECURSIVE: 4,
          REVOKED: 5
        }

        const dataToSign = keysToUpload.map(({ keyData, rollingStartIntervalNumber, rollingPeriod, transmissionRiskLevel, visitedCountries, origin, reportType, days_since_onset_of_symptoms }) => {
          const rollingStartIntervalNumberBuffer = Buffer.alloc(4)
          const rollingPeriodBuffer = Buffer.alloc(4)
          const transmissionRiskLevelBuffer = Buffer.alloc(4)
          const reportTypeBuffer = Buffer.alloc(4)
          const daysSinceOnsetOfSymptomsBuffer = Buffer.alloc(4)

          let data = ''

          rollingStartIntervalNumberBuffer.writeUInt32BE(rollingStartIntervalNumber)
          rollingPeriodBuffer.writeUInt32BE(rollingPeriod)
          transmissionRiskLevelBuffer.writeInt32BE(transmissionRiskLevel)
          reportTypeBuffer.writeInt32BE(reportTypes[reportType] || 0)
          daysSinceOnsetOfSymptomsBuffer.writeUInt32BE(days_since_onset_of_symptoms)

          data += keyData
          data += '.'

          data += rollingStartIntervalNumberBuffer.toString('base64')
          data += '.'

          data += rollingPeriodBuffer.toString('base64')
          data += '.'

          data += transmissionRiskLevelBuffer.toString('base64')
          data += '.'

          data += Buffer.from(visitedCountries.join(','), 'utf-8').toString('base64')
          data += '.'

          data += Buffer.from(origin, 'utf-8').toString('base64')
          data += '.'

          data += reportTypeBuffer.toString('base64')
          data += '.'

          data += daysSinceOnsetOfSymptomsBuffer.toString('base64')
          data += '.'

          return data
        })

        const sortedDataToSign = dataToSign.sort((a, b) => {
          const encodedA = Buffer.from(a, 'utf-8').toString('base64')
          const encodedB = Buffer.from(b, 'utf-8').toString('base64')

          if (encodedA < encodedB) {
            return -1
          }

          if (encodedA > encodedB) {
            return 1
          }

          return 0
        })

        const signed = jsrsasign.KJUR.asn1.cms.CMSUtil.newSignedData({
          content: { hex: Buffer.from(sortedDataToSign.join(''), 'utf-8').toString('hex') },
          certs: [sign.cert],
          detached: true,
          signerInfos: [{
            hashAlg: 'sha256',
            sAttr: {
              SigningTime: {}
            },
            signerCert: sign.cert,
            sigAlg: 'SHA1withRSA',
            signerPrvKey: sign.key
          }]
        })

        await axios.post(
          `${url}/diagnosiskeys/upload`,
          { keys: keysToUpload },
          {
            headers: {
              'Content-Type': 'application/json; version=1.0',
              batchTag,
              batchSignature: Buffer.from(signed.getContentInfoEncodedHex(), 'hex').toString('base64')
            },
            httpsAgent
          }
        )

        await client.query('COMMIT')

        console.log(
          `uploaded ${keysToUpload.length} to batch ${batchTag}`
        )
      } catch (err) {
        if (err.response && err.response.data) {
          console.log(err.response.data)
        }

        await client.query('ROLLBACK')
        throw err
      }
    }
  }
}

exports.handler = async function () {
  const { efgs, servers } = await getInteropConfig()

  await withDatabase(async client => {
    for (const { id, privateKey, token, url } of servers) {
      await uploadToInterop(client, id, privateKey, token, url)
    }

    if (efgs && efgs.upload) {
      await uploadToEfgs(client, efgs)
    }
  })
}

runIfDev(exports.handler)
