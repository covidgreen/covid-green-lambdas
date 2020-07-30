const AWS = require('aws-sdk')
const jwt = require('jsonwebtoken')
const pg = require('pg')

const isProduction = /^\s*production\s*$/i.test(process.env.NODE_ENV)
const ssm = new AWS.SSM({ region: process.env.AWS_REGION })
const secretsManager = new AWS.SecretsManager({ region: process.env.AWS_REGION })

async function getParameter(id) {
  const response = await ssm
    .getParameter({ Name: `${process.env.CONFIG_VAR_PREFIX}${id}` })
    .promise()

  return response.Parameter.Value
}

async function getSecret(id) {
  const response = await secretsManager
    .getSecretValue({ SecretId: `${process.env.CONFIG_VAR_PREFIX}${id}` })
    .promise()

  return JSON.parse(response.SecretString)
}

async function getAssetsBucket() {
  if (isProduction) {
    return await getParameter('s3_assets_bucket')
  } else {
    return process.env.ASSETS_BUCKET
  }
}

async function getDatabase() {
  require('pg-range').install(pg)

  let client

  if (isProduction) {
    const [{ username: user, password }, host, port, ssl, database] = await Promise.all([
      getSecret('rds-read-write'),
      getParameter('db_host'),
      getParameter('db_port'),
      getParameter('db_ssl'),
      getParameter('db_database')
    ])

    client = new pg.Client({
      host,
      database,
      user,
      password,
      port: Number(port),
      ssl: ssl === 'true'
    })
  } else {
    const { user, password, host, port, ssl, database } = {
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      host: process.env.DB_HOST,
      port: Number(process.env.DB_PORT),
      ssl:  /true/i.test(process.env.DB_SSL),
      database: process.env.DB_DATABASE
    }

    client = new pg.Client({
      host,
      database,
      user,
      password,
      port: Number(port),
      ssl: ssl === 'true'
    })
  }

  await client.connect()

  return client
}

async function getExposuresConfig() {
  if (isProduction) {
    const [
      { privateKey, signatureAlgorithm, verificationKeyId, verificationKeyVersion },
      appBundleId,
      defaultRegion,
      nativeRegions
    ] = await Promise.all([
      getSecret('exposures'),
      getParameter('app_bundle_id'),
      getParameter('default_region'),
      getParameter('native_regions')
    ])

    return {
      appBundleId,
      defaultRegion,
      nativeRegions: nativeRegions.split(','),
      privateKey,
      signatureAlgorithm,
      verificationKeyId,
      verificationKeyVersion
    }
  } else {
    return {
      appBundleId: process.env.APP_BUNDLE_ID,
      defaultRegion: process.env.EXPOSURES_DEFAULT_REGION,
      nativeRegions: process.env.EXPOSURES_NATIVE_REGIONS.split(','),
      privateKey: process.env.EXPOSURES_PRIVATE_KEY,
      signatureAlgorithm: process.env.EXPOSURES_SIGNATURE_ALGORITHM,
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
      maxAge: Number(process.env.INTEROP_MAX_AGE),
      privateKey: process.env.INTEROP_PRIVATE_KEY,
      token: process.env.INTEROP_TOKEN,
      url: process.env.INTEROP_URL
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
  getAssetsBucket,
  getDatabase,
  getExposuresConfig,
  getInteropConfig,
  getJwtSecret,
  isAuthorized,
  runIfDev
}
