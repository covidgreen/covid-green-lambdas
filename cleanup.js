const SQL = require('@nearform/sql')
const { getDatabase, getExpiryConfig, runIfDev } = require('./utils')

async function createRegistrationMetrics(client) {
  const sql = SQL`
    INSERT INTO metrics (date, event, os, version, value)
    SELECT CURRENT_DATE, 'REGISTER', '', '', COUNT(id)
    FROM registrations WHERE created_at::DATE = CURRENT_DATE
    ON CONFLICT ON CONSTRAINT metrics_pkey
    DO UPDATE SET value = EXCLUDED.value
    RETURNING value
  `

  const { rows } = await client.query(sql)
  const [{ value }] = rows

  console.log(`updated register metric for today with value ${value}`)
}

async function removeExpiredCodes(client, codeLifetime) {
  const sql = SQL`
    DELETE FROM verifications
    WHERE created_at < CURRENT_TIMESTAMP - ${`${codeLifetime} mins`}::INTERVAL
  `

  const { rowCount } = await client.query(sql)

  console.log(`deleted ${rowCount} codes older than ${codeLifetime} minutes`)
}

async function removeExpiredTokens(client, tokenLifetime) {
  const sql = SQL`
    DELETE FROM upload_tokens
    WHERE created_at < CURRENT_TIMESTAMP - ${`${tokenLifetime} mins`}::INTERVAL
  `

  const { rowCount } = await client.query(sql)

  console.log(`deleted ${rowCount} tokens older than ${tokenLifetime} minutes`)
}

async function removeOldNoticesKeys(client, noticeLifetime) {
  const sql = SQL`
    DELETE FROM notices
    WHERE created_at < CURRENT_TIMESTAMP - ${`${noticeLifetime} mins`}::INTERVAL
  `

  const { rowCount } = await client.query(sql)

  console.log(
    `deleted ${rowCount} notices keys older than ${noticeLifetime} minutes`
  )
}

exports.handler = async function() {
  const client = await getDatabase()
  const {
    codeLifetime,
    tokenLifetime,
    noticeLifetime
  } = await getExpiryConfig()

  await createRegistrationMetrics(client)
  await removeExpiredCodes(client, codeLifetime)
  await removeExpiredTokens(client, tokenLifetime)
  await removeOldNoticesKeys(client, noticeLifetime)

  return true
}

runIfDev(exports.handler)
