const AWS = require('aws-sdk')
const archiver = require('archiver')
const crypto = require('crypto')
const fs = require('fs')
const protobuf = require('protobufjs')
const SQL = require('@nearform/sql')
const {
  withDatabase,
  getAssetsBucket,
  getExposuresConfig,
  runIfDev
} = require('./utils')
const { dirname } = require('path')

async function clearExpiredFiles(client, s3, bucket, lastExposureId) {
  const query = SQL`
    DELETE FROM exposure_export_files
    WHERE last_exposure_id <= ${lastExposureId}
    RETURNING id, path
  `

  const promises = []
  const { rows } = await client.query(query)

  for (const { id, path } of rows) {
    console.log(`removing old file ${id} with path ${path}`)

    const fileObject = {
      Bucket: bucket,
      Key: path
    }

    promises.push(s3.deleteObject(fileObject).promise())
  }

  await Promise.all(promises)
}

async function clearExpiredExposures(client, s3, bucket) {
  const query = SQL`
    DELETE FROM exposures
    WHERE created_at < CURRENT_DATE - INTERVAL '14 days'
    RETURNING id
  `

  const { rows } = await client.query(query)

  await clearExpiredFiles(
    client,
    s3,
    bucket,
    rows.reduce((max, { id }) => Math.max(max, id), 0)
  )
}

async function uploadFile(
  firstExposureId,
  client,
  s3,
  bucket,
  config,
  endDate
) {
  const {
    defaultRegion,
    nativeRegions,
    privateKey,
    ...signatureInfoPayload
  } = config
  const results = {}
  const exposures = await getExposures(client, firstExposureId, config, endDate)

  let firstExposureCreatedAt = null
  let lastExposureCreatedAt = null
  let lastExposureId = 0
  let startExposureId = null

  for (const { id, created_at: createdAt, regions, ...exposure } of exposures) {
    if (id > lastExposureId) {
      lastExposureId = id
    }
    if (startExposureId === null || id < startExposureId) {
      startExposureId = id
    }
    if (firstExposureCreatedAt === null || createdAt < firstExposureCreatedAt) {
      firstExposureCreatedAt = createdAt
    }

    if (lastExposureCreatedAt === null || createdAt > lastExposureCreatedAt) {
      lastExposureCreatedAt = createdAt
    }

    if (results[defaultRegion] === undefined) {
      results[defaultRegion] = []
    }

    results[defaultRegion].push(exposure)
  }

  for (const [region, exposures] of Object.entries(results)) {
    if (
      await exposureFileExists(client, startExposureId, lastExposureId, region)
    ) {
      console.log(
        `file for ${region} exposures ${startExposureId} to ${lastExposureId} already exists`
      )
    } else {
      console.log(
        `generating file for ${region} exposures ${startExposureId} to ${lastExposureId}`
      )

      const now = new Date()
      const path = `exposures/${region.toLowerCase()}/${now.getTime()}.zip`

      const data = await createExportFile(
        privateKey,
        signatureInfoPayload,
        exposures,
        region,
        1,
        1,
        firstExposureCreatedAt,
        lastExposureCreatedAt
      )

      if (bucket) {
        const exportFileObject = {
          ACL: 'private',
          Body: data,
          Bucket: bucket,
          ContentType: 'application/zip',
          Key: path
        }

        await s3.putObject(exportFileObject).promise()
      } else {
        fs.mkdirSync(dirname(`./out/${path}`), { recursive: true })
        fs.writeFileSync(`./out/${path}`, data)
      }

      const query = SQL`
        INSERT INTO exposure_export_files (path, exposure_count, since_exposure_id, last_exposure_id, first_exposure_created_at, region)
        VALUES (${path}, ${exposures.length}, ${startExposureId}, ${lastExposureId}, ${firstExposureCreatedAt}, ${region})
      `

      await client.query(query)
    }
  }
}

function formatDate(date) {
  return `${date.getFullYear()}-${date.getMonth() + 1}-${date.getDate()}`
}

async function getExposures(client, since, config, endDate) {
  const query = SQL`
    SELECT
      id,
      created_at,
      key_data,
      rolling_period,
      rolling_start_number,
      transmission_risk_level,
      regions,
      days_since_onset
    FROM exposures
    WHERE id > ${since} 
  `

  if (endDate) {
    query.append(SQL` AND created_at < ${formatDate(endDate)}`)
  }
  query.append(SQL` ORDER BY key_data ASC`)

  const { rows } = await client.query(query)
  const exposures = []

  for (const row of rows) {
    const endDate = new Date(
      (row.rolling_start_number + row.rolling_period) * 1000 * 600 +
        config.varianceOffsetMins * 1000 * 60
    )

    if (config.disableValidKeyCheck === false && endDate > new Date()) {
      console.log(
        `re-inserting key ${row.id}, ${row.key_data} for future processing as it is still valid until ${endDate}`
      )

      await client.query(SQL`
        WITH deleted AS (
          DELETE FROM exposures
          WHERE id = ${row.id}
          RETURNING
            key_data,
            rolling_period,
            rolling_start_number,
            transmission_risk_level,
            regions,
            test_type,
            origin,
            days_since_onset
        )
        INSERT INTO exposures (
          key_data,
            rolling_period,
            rolling_start_number,
            transmission_risk_level,
            regions,
            test_type,
            origin,
            days_since_onset
        )
        SELECT
          key_data,
          rolling_period,
          rolling_start_number,
          transmission_risk_level,
          regions,
          test_type,
          origin,
          days_since_onset
        FROM deleted
      `)
    } else {
      exposures.push(row)
    }
  }

  return exposures
}

function createExportFile(
  privateKey,
  signatureInfoPayload,
  exposures,
  region,
  batchNum,
  batchSize,
  startDate,
  endDate
) {
  // eslint-disable-next-line no-async-promise-executor
  return new Promise(async (resolve) => {
    const root = await protobuf.load('exposures.proto')
    const tekExport = root.lookupType('TemporaryExposureKeyExport')
    const signatureList = root.lookupType('TEKSignatureList')
    const sign = crypto.createSign('sha256')

    const keys = exposures.map(
      ({
        key_data: keyData,
        rolling_start_number: rollingStartIntervalNumber,
        transmission_risk_level: transmissionRiskLevel,
        rolling_period: rollingPeriod,
        days_since_onset: daysSinceOnsetOfSymptoms
      }) => ({
        keyData,
        rollingStartIntervalNumber,
        transmissionRiskLevel:
          transmissionRiskLevel > 8 || transmissionRiskLevel < 0
            ? 0
            : transmissionRiskLevel,
        rollingPeriod,
        reportType: 1,
        daysSinceOnsetOfSymptoms: Math.min(
          Math.max(daysSinceOnsetOfSymptoms, -14),
          14
        )
      })
    )

    const filteredKeys = keys.filter(({ keyData }) => {
      const decodedKeyData = Buffer.from(keyData, 'base64')

      if (decodedKeyData.length !== 16) {
        console.log(
          `excluding invalid key ${keyData}, length was ${decodedKeyData.length}`
        )

        return false
      }

      return true
    })

    const tekExportPayload = {
      startTimestamp: Math.floor(startDate / 1000),
      endTimestamp: Math.floor(endDate / 1000),
      region,
      batchNum,
      batchSize,
      signatureInfos: [signatureInfoPayload],
      keys: filteredKeys,
      revisedKeys: []
    }

    const tekExportMessage = tekExport.create(tekExportPayload)
    const tekExportEncoded = tekExport.encode(tekExportMessage).finish()

    const tekExportData = Buffer.concat([
      Buffer.from('EK Export v1'.padEnd(16), 'utf8'),
      tekExportEncoded
    ])

    sign.update(tekExportData)
    sign.end()

    const signature = sign.sign({
      key: privateKey,
      dsaEncoding: 'der'
    })

    const signatureListPayload = {
      signatures: [
        {
          signatureInfo: signatureInfoPayload,
          batchNum,
          batchSize,
          signature
        }
      ]
    }

    const signatureListMessage = signatureList.create(signatureListPayload)
    const signatureListEncoded = signatureList
      .encode(signatureListMessage)
      .finish()

    const archive = archiver('zip')
    let output = Buffer.alloc(0)

    archive.on('data', (data) => {
      output = Buffer.concat([output, data])
    })

    archive.on('finish', () => {
      resolve(output)
    })

    archive.append(tekExportData, { name: 'export.bin' })
    archive.append(signatureListEncoded, { name: 'export.sig' })
    archive.finalize()
  })
}

async function exposureFileExists(
  client,
  firstExposureId,
  lastExposureId,
  region
) {
  const query = SQL`
    SELECT id FROM exposure_export_files
    WHERE since_exposure_id = ${firstExposureId}
    AND last_exposure_id = ${lastExposureId}
    AND region = ${region}
  `

  const { rowCount } = await client.query(query)

  return rowCount > 0
}

async function uploadExposuresSince(
  client,
  s3,
  bucket,
  config,
  since,
  endDate,
  dateRangeOnly
) {
  let startId = 0

  if (!dateRangeOnly) {
    const query = SQL`
      SELECT COALESCE(MAX(last_exposure_id), 0) AS "firstExposureId"
      FROM exposure_export_files
      WHERE created_at < ${since}
      `

    const { rows } = await client.query(query)
    const [{ firstExposureId }] = rows

    startId = firstExposureId
  }

  if (startId === 0) {
    const query = SQL`
      SELECT COALESCE(MIN(id), 0) - 1 AS "firstExposureId"
      FROM exposures
      WHERE created_at >= ${formatDate(since)}
      `
    const { rows } = await client.query(query)
    const [{ firstExposureId }] = rows
    startId = firstExposureId
  }
  await uploadFile(startId, client, s3, bucket, config, endDate)
}

exports.handler = async function () {
  const s3 = new AWS.S3({ region: process.env.AWS_REGION })
  const bucket = await getAssetsBucket()
  const config = await getExposuresConfig()
  const startDate = new Date()
  startDate.setHours(0, 0, 0, 0)
  startDate.setDate(startDate.getDate() - 14)

  const endDate = new Date(startDate)
  endDate.setDate(endDate.getDate() + 1)

  await withDatabase(async (client) => {
    await clearExpiredExposures(client, s3, bucket)

    console.log('Creating latest export file for ', new Date())
    await uploadExposuresSince(client, s3, bucket, config, new Date())

    for (let i = 0; i < 14; i++) {
      console.log('Creating export file for ', startDate, endDate)
      await uploadExposuresSince(
        client,
        s3,
        bucket,
        config,
        startDate,
        endDate,
        true
      )
      startDate.setDate(startDate.getDate() + 1)
      endDate.setDate(endDate.getDate() + 1)
    }
  })

  return true
}

runIfDev(exports.handler)
