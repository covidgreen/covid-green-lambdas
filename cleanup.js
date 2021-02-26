const SQL = require('@nearform/sql')
const AWS = require('aws-sdk')
const { utcToZonedTime } = require('date-fns-tz')
const fetch = require('node-fetch')

const {
  withDatabase,
  getExpiryConfig,
  getTimeZone,
  getENXLogoEnabled,
  getAPHLServerDetails,
  runIfDev
} = require('./utils')

async function createAPHLVerificationServerMetrics(client) {
  const details = await getAPHLServerDetails()

  if (details.server === '') {
    console.log('No APHL server configured, ignoring stats')
    return
  }

  const response = await fetch(`${details.server}/api/stats/realm.json`, {
    method: 'GET',
    headers: {
      'X-API-Key': details.key,
      'Content-Type': 'application/json'
    }
  })

  if (response.status === 200) {
    const data = await response.json()

    const runDate = new Date()
    runDate.setHours(0, 0, 0, 0)
    runDate.setDate(runDate.getDate() - 1)
    const dataSet = data.statistics.filter(
      s => new Date(s.date.substring(0, 10)).getTime() >= runDate.getTime()
    )

    for (let i = 0; i < dataSet.length; i++) {
      const metricsDate = dataSet[i].date
      const metrics = dataSet[i].data

      const sql = SQL`
      INSERT INTO metrics (date, event, os, version, value)
      VALUES (${metricsDate}, 'APHL_CODES_ISSUES', '', '', ${metrics.codes_issued}),
      (${metricsDate}, 'APHL_CODES_CLAIMED', '', '', ${metrics.codes_claimed}),
      (${metricsDate}, 'APHL_CODES_INVALID', '', '', ${metrics.codes_invalid}),
      (${metricsDate}, 'APHL_CLAIM_MEAN_AGE_SECONDS', '', '', ${metrics.code_claim_mean_age_seconds})
      ON CONFLICT ON CONSTRAINT metrics_pkey
      DO UPDATE SET value = EXCLUDED.value
      WHERE metrics.date = EXCLUDED.date AND metrics.event = EXCLUDED.event `

      await client.query(sql)
    }
  }
}

async function createRegistrationMetrics(client) {
  const timeZone = await getTimeZone()

  const sql = SQL`
    INSERT INTO metrics (date, event, os, version, value)
    SELECT
      (CURRENT_TIMESTAMP AT TIME ZONE ${timeZone})::DATE,
      'REGISTER',
      '',
      '',
      COUNT(id)
    FROM registrations
    WHERE
      (nonce != '123456' OR nonce IS NULL)
    AND
      (created_at AT TIME ZONE ${timeZone})::DATE =
      (CURRENT_TIMESTAMP AT TIME ZONE ${timeZone})::DATE
    ON CONFLICT ON CONSTRAINT metrics_pkey
    DO UPDATE SET value = EXCLUDED.value
    RETURNING value`

  const { rows } = await client.query(sql)
  const [{ value }] = rows

  console.log(`updated register metric for today with value ${value}`)
}

async function storeENXLogoRequests(client, metrics) {
  const timeZone = await getTimeZone()

  // include zero metrics also for now
  const nonZeroMetrics = metrics // .filter(m => m.value > 0)

  const sql = SQL`
    INSERT INTO metrics (date, event, os, version, value)
    VALUES `

  nonZeroMetrics.forEach((metric, index) => {
    sql.append(
      SQL`((CURRENT_TIMESTAMP AT TIME ZONE ${timeZone})::DATE, ${metric.metric}, '', '', ${metric.value})`
    )
    if (index < nonZeroMetrics.length - 1) {
      sql.append(SQL`,`)
    }
  })

  sql.append(SQL`
    ON CONFLICT ON CONSTRAINT metrics_pkey
    DO UPDATE SET value = EXCLUDED.value
    WHERE metrics.date = EXCLUDED.date AND metrics.event = EXCLUDED.event
  `)

  if (nonZeroMetrics.length > 0) {
    await client.query(sql)
  }
}

function buildMetricsQuery() {
  const metrics = [
    { metric: 'enxlogoall', label: 'ENX_LOGO_REQUESTS_ALL' },
    { metric: 'enxlogo200', label: 'ENX_LOGO_REQUESTS_200' },
    { metric: 'enxlogo304', label: 'ENX_LOGO_REQUESTS_304' },
    { metric: 'enxlogosettings', label: 'ENX_LOGO_REQUESTS_SETTINGS' },
    { metric: 'enxlogoenbuddy', label: 'ENX_LOGO_REQUESTS_ENBUDDY' },
    { metric: 'enxlogoenhealthbuddy', label: 'ENX_LOGO_REQUESTS_ENHEALTHBUDDY' },
    { metric: 'enxlogosettigs2', label: 'ENX_LOGO_REQUESTS_SETTINGS2' }
  ]
  const metricsData = []

  metrics.forEach(m => {
    metricsData.push({
      Id: `en_${m.metric}`,
      MetricStat: {
        Metric: {
          Namespace: 'ApiGateway',
          MetricName: m.metric
        },
        Period: 86400,
        Stat: 'Sum'
      },
      Label: `${m.label}`,
      ReturnData: true
    })
  })

  return metricsData
}

async function createENXLogoMetrics(client, event) {
  const timeZone = await getTimeZone()
  const enxLogoEnabled = await getENXLogoEnabled()

  if (!enxLogoEnabled) {
    console.log('Skipping enx logo checks, not enabled')
    return
  }

  const cw = new AWS.CloudWatch()

  let startDate = new Date()
  startDate.setHours(0, 0, 0, 0)

  if (event && event.startDate) {
    startDate = new Date(event.startDate)
  }
  const endDate = new Date(startDate)
  endDate.setHours(0, 0, 0, 0)
  endDate.setDate(endDate.getDate() + 1)

  const params = {
    MetricDataQueries: buildMetricsQuery(),
    StartTime: utcToZonedTime(startDate, timeZone),
    EndTime: utcToZonedTime(endDate, timeZone)
  }
  const logData = await new Promise((resolve, reject) => {
    cw.getMetricData(params, function(err, data) {
      if (err) {
        console.log(err) // an error occurred
        reject(err)
      } else {
        resolve(data)
      }
    })
  })

  const results = logData.MetricDataResults
  const dbMetrics = []

  results.forEach(response => {
    dbMetrics.push({
      metric: response.Label,
      value:
        response.Values && response.Values.length > 0 ? response.Values[0] : 0
    })
  })
  await storeENXLogoRequests(client, dbMetrics)
  console.log('updated enx logo requests metrics', startDate, dbMetrics)
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

exports.handler = async function(event) {
  const {
    codeLifetime,
    tokenLifetime,
    noticeLifetime
  } = await getExpiryConfig()

  await withDatabase(async client => {
    await createRegistrationMetrics(client)
    await removeExpiredCodes(client, codeLifetime)
    await removeExpiredTokens(client, tokenLifetime)
    await removeOldNoticesKeys(client, noticeLifetime)
    await createENXLogoMetrics(client, event)
    await createAPHLVerificationServerMetrics(client)
  })

  return true
}

runIfDev(exports.handler)
