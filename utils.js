const AWS = require('aws-sdk')
const SQL = require('@nearform/sql')
const fetch = require('node-fetch')
const jwt = require('jsonwebtoken')
const pg = require('pg')
require('pg-range').install(pg)

const isProduction = /^\s*production\s*$/i.test(process.env.NODE_ENV)
const ssm = new AWS.SSM({ region: process.env.AWS_REGION })
const secretsManager = new AWS.SecretsManager({
  region: process.env.AWS_REGION
})

async function getParameter(id, defaultValue) {
  try {
    const response = await ssm
      .getParameter({ Name: `${process.env.CONFIG_VAR_PREFIX}${id}` })
      .promise()

    return response.Parameter.Value
  } catch (err) {
    if (defaultValue !== undefined) {
      return defaultValue
    }

    throw err
  }
}

async function getSecret(id) {
  const response = await secretsManager
    .getSecretValue({ SecretId: `${process.env.CONFIG_VAR_PREFIX}${id}` })
    .promise()

  return JSON.parse(response.SecretString)
}

async function getAlertConfig() {
  if (isProduction) {
    const [emailAddress, sender] = await Promise.all([
      getParameter('qr_alert_email'),
      getParameter('qr_sender')
    ])

    return { emailAddress, sender }
  } else {
    return {
      emailAddress: process.env.QR_ALERT_EMAIL,
      sender: process.env.QR_SENDER
    }
  }
}

async function getAssetsBucket() {
  if (isProduction) {
    return await getParameter('s3_assets_bucket')
  } else {
    return process.env.ASSETS_BUCKET
  }
}

async function getExpiryConfig() {
  if (isProduction) {
    const [codeLifetime, tokenLifetime, noticeLifetime] = await Promise.all([
      getParameter('security_code_removal_mins'),
      getParameter('upload_token_lifetime_mins'),
      getParameter('self_isolation_notice_lifetime_mins', 20160)
    ])

    return { codeLifetime, tokenLifetime, noticeLifetime }
  } else {
    return {
      codeLifetime: process.env.CODE_LIFETIME_MINS,
      tokenLifetime: process.env.UPLOAD_TOKEN_LIFETIME_MINS,
      noticeLifetime: process.env.NOTICE_LIFETIME_MINS
    }
  }
}

async function getProdDbConfig() {
  const [
    { username: user, password },
    host,
    port,
    ssl,
    database
  ] = await Promise.all([
    getSecret('rds-read-write'),
    getParameter('db_host'),
    getParameter('db_port'),
    getParameter('db_ssl'),
    getParameter('db_database')
  ])

  const options = {
    host,
    database,
    user,
    password,
    port: Number(port)
  }

  if (/true/i.test(ssl)) {
    const certResponse = await fetch(
      'https://s3.amazonaws.com/rds-downloads/rds-combined-ca-bundle.pem'
    )
    const certBody = await certResponse.text()

    options.ssl = {
      ca: [certBody],
      rejectUnauthorized: true
    }
  } else {
    options.ssl = false
  }

  return options
}

function getDevDbConfig() {
  return {
    host: process.env.DB_HOST,
    database: process.env.DB_DATABASE,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    port: Number(process.env.DB_PORT),
    ssl: /true/i.test(process.env.DB_SSL)
      ? { rejectUnauthorized: false }
      : false
  }
}

async function getDbConfig() {
  return isProduction ? getProdDbConfig() : getDevDbConfig()
}

async function withDatabase(fn) {
  const options = await getDbConfig()
  const client = new pg.Client(options)

  await client.connect()

  try {
    return await fn(client)
  } finally {
    await client.end()
  }
}

async function getExposuresConfig() {
  if (isProduction) {
    const [
      {
        privateKey,
        signatureAlgorithm,
        verificationKeyId,
        verificationKeyVersion
      },
      defaultRegion,
      disableValidKeyCheck,
      nativeRegions,
      varianceOffsetMins
    ] = await Promise.all([
      getSecret('exposures'),
      getParameter('default_region'),
      getParameter('disable_valid_key_check'),
      getParameter('native_regions'),
      getParameter('variance_offset_mins')
    ])

    return {
      defaultRegion,
      disableValidKeyCheck: /true/i.test(disableValidKeyCheck),
      nativeRegions: nativeRegions.split(','),
      privateKey,
      signatureAlgorithm,
      varianceOffsetMins: Number(varianceOffsetMins),
      verificationKeyId,
      verificationKeyVersion
    }
  } else {
    return {
      defaultRegion: process.env.EXPOSURES_DEFAULT_REGION,
      disableValidKeyCheck: /true/i.test(process.env.DISABLE_VALID_KEY_CHECK),
      nativeRegions: process.env.EXPOSURES_NATIVE_REGIONS.split(','),
      privateKey: process.env.EXPOSURES_PRIVATE_KEY,
      signatureAlgorithm: process.env.EXPOSURES_SIGNATURE_ALGORITHM,
      varianceOffsetMins: Number(process.env.VARIANCE_OFFSET_MINS),
      verificationKeyId: process.env.EXPOSURES_KEY_ID,
      verificationKeyVersion: process.env.EXPOSURES_KEY_VERSION
    }
  }
}

async function getInteropConfig() {
  if (isProduction) {
    return await getSecret('interop')
  } else {
    return {
      efgs: {
        url: process.env.EFGS_URL,
        download: /true/i.test(process.env.EFGS_DOWNLOAD),
        upload: /true/i.test(process.env.EFGS_UPLOAD),
        auth: {
          cert: process.env.EFGS_AUTH_CERT,
          key: process.env.EFGS_AUTH_KEY
        },
        sign: {
          cert: process.env.EFGS_SIGN_CERT,
          key: process.env.EFGS_SIGN_KEY
        }
      },
      servers: [
        {
          id: process.env.INTEROP_SERVER_ID,
          maxAge: Number(process.env.INTEROP_MAX_AGE),
          privateKey: process.env.INTEROP_PRIVATE_KEY,
          token: process.env.INTEROP_TOKEN,
          url: process.env.INTEROP_URL
        }
      ]
    }
  }
}

async function getJwtSecret() {
  if (isProduction) {
    const { key } = await getSecret('jwt')

    return key
  } else {
    return process.env.JWT_SECRET
  }
}

async function getTimeZone() {
  if (isProduction) {
    return await getParameter('time_zone', 'UTC')
  } else {
    return process.env.TIME_ZONE
  }
}

async function insertMetric(client, event, os, version, value = 1) {
  const timeZone = await getTimeZone()

  const query = SQL`
    INSERT INTO metrics (date, event, os, version, value)
    VALUES (
      (CURRENT_TIMESTAMP AT TIME ZONE ${timeZone})::DATE,
      ${event},
      ${os},
      ${version},
      ${value}
    )
    ON CONFLICT ON CONSTRAINT metrics_pkey
    DO UPDATE SET value = metrics.value + ${value}`

  await client.query(query)
}

function isAuthorized(token, secret) {
  try {
    const data = jwt.verify(token.replace(/^Bearer /, ''), secret)

    if (data.refresh || !data.id) {
      return false
    }

    return true
  } catch (error) {
    return false
  }
}

async function getQrConfig() {
  if (isProduction) {
    const [appUrl, bucket, sender] = await Promise.all([
      getParameter('qr_generate_url'),
      getParameter('s3_qr_bucket')
    ])

    return { appUrl, bucket, sender }
  } else {
    return {
      appUrl: process.env.QR_APP_URL,
      bucket: process.env.QR_BUCKET_NAME
    }
  }
}

function runIfDev(fn) {
  if (!isProduction) {
    fn(JSON.parse(process.argv[2] || '{}'))
      .then(result => {
        console.log(result)
        process.exit(0)
      })
      .catch(error => {
        console.log(error)
        process.exit(1)
      })
  }
}

module.exports = {
  withDatabase,
  getAlertConfig,
  getAssetsBucket,
  getExpiryConfig,
  getExposuresConfig,
  getInteropConfig,
  getJwtSecret,
  getQrConfig,
  getTimeZone,
  insertMetric,
  isAuthorized,
  runIfDev
}
