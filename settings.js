const AWS = require('aws-sdk')
const SQL = require('@nearform/sql')
const crypto = require('crypto')
const querystring = require('querystring')
const { unflatten } = require('flat')
const { withDatabase, getAssetsBucket, runIfDev } = require('./utils')

async function getSettingsBody(client) {
  const sql = SQL`SELECT is_language, settings_key, settings_value FROM settings`
  const { rows } = await client.query(sql)

  const result = {
    exposures: {},
    language: {}
  }

  for (const {
    is_language: isLanguage,
    settings_key: settingsKey,
    settings_value: settingsValue
  } of rows) {
    result[isLanguage ? 'language' : 'exposures'][settingsKey] = settingsValue
  }

  return unflatten(result)
}

async function isChanged(s3, bucket, key, hash) {
  try {
    const { TagSet } = await s3
      .getObjectTagging({ Bucket: bucket, Key: key })
      .promise()

    for (const { Key, Value } of TagSet) {
      if (Key === 'Hash') {
        return Value !== hash
      }
    }

    return true
  } catch (error) {
    return true
  }
}

async function updateIfChanged(s3, bucket, key, data) {
  const md5 = crypto.createHash('md5')
  const hash = md5.update(JSON.stringify(data)).digest('hex')

  if (await isChanged(s3, bucket, key, hash)) {
    console.log(`writing ${key} with hash ${hash}`)

    const object = {
      ACL: 'private',
      Body: Buffer.from(JSON.stringify({ generatedAt: new Date(), ...data })),
      Bucket: bucket,
      ContentType: 'application/json',
      Key: key,
      Tagging: querystring.stringify({
        Hash: hash
      })
    }

    await s3.putObject(object).promise()
  } else {
    console.log(`file ${key} has not changed`)
  }
}

exports.handler = async function () {
  const s3 = new AWS.S3({ region: process.env.AWS_REGION })
  const bucket = await getAssetsBucket()

  return await withDatabase(async (client) => {
    const { exposures, language } = await getSettingsBody(client)

    await updateIfChanged(s3, bucket, 'exposures.json', exposures)
    await updateIfChanged(s3, bucket, 'language.json', language)
    await updateIfChanged(s3, bucket, 'settings.json', {
      ...exposures,
      ...language
    })

    return { exposures, language }
  })
}

runIfDev(exports.handler)
