const SQL = require('@nearform/sql')
const AWS = require('aws-sdk')
const { zonedTimeToUtc, utcToZonedTime } = require('date-fns-tz')
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
      (s) => new Date(s.date.substring(0, 10)).getTime() >= runDate.getTime()
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
      (created_at AT TIME ZONE ${timeZone})::DATE as groupDate,
      'REGISTER',
      '',
      '',
      COUNT(id)
    FROM registrations
    WHERE
      (nonce != '123456' OR nonce IS NULL)
    AND
      (
        ((created_at AT TIME ZONE ${timeZone})::DATE =
        (CURRENT_TIMESTAMP AT TIME ZONE ${timeZone})::DATE) OR 
        ((created_at AT TIME ZONE ${timeZone})::DATE =
        (CURRENT_TIMESTAMP AT TIME ZONE ${timeZone})::DATE - 1)
      )
    GROUP BY groupDate
    ON CONFLICT ON CONSTRAINT metrics_pkey
    DO UPDATE SET value = EXCLUDED.value
    RETURNING value`

  const { rows } = await client.query(sql)

  console.log(`updated register metric for last 2 days ${rows.length}`)
}

async function storeENXHourlyData(client, metrics) {
  const dbData = {}

  metrics.forEach((metric) => {
    if (!dbData[metric.date]) {
      dbData[metric.date] = {}
    }
    dbData[metric.date][metric.metric] = metric.value || 0
  })
  const sql = SQL`
    INSERT INTO enx_onboarding_requests (event_date, all_counts, success_counts, settings_counts, enbuddy_counts, healthenbuddy_counts)
    VALUES `

  Object.keys(dbData).forEach((key, index) => {
    const metric = dbData[key]
    sql.append(
      SQL`(${new Date(key)}, ${metric.ENX_LOGO_REQUESTS_ALL || 0}, ${
        metric.ENX_LOGO_REQUESTS_200 || 0
      }, ${metric.ENX_LOGO_REQUESTS_SETTINGS || 0}, ${
        metric.ENX_LOGO_REQUESTS_ENBUDDY || 0
      }, ${metric.ENX_LOGO_REQUESTS_HEALTHENBUDDY || 0})`
    )
    if (index < Object.keys(dbData).length - 1) {
      sql.append(SQL`,`)
    }
  })

  sql.append(SQL`
    ON CONFLICT ON CONSTRAINT enx_onboarding_requests_event_date_key
    DO UPDATE SET all_counts = EXCLUDED.all_counts, success_counts = EXCLUDED.success_counts, settings_counts = EXCLUDED.settings_counts,
      enbuddy_counts = EXCLUDED.enbuddy_counts, healthenbuddy_counts = EXCLUDED.healthenbuddy_counts
    WHERE enx_onboarding_requests.event_date = EXCLUDED.event_date
  `)
  if (Object.keys(dbData).length > 0) {
    await client.query(sql)
  }
}

async function storeENXLogoRequests(client, metrics) {
  // include zero metrics also for now
  const nonZeroMetrics = metrics // .filter(m => m.value > 0)

  const sql = SQL`
    INSERT INTO metrics (date, event, os, version, value)
    VALUES `

  nonZeroMetrics.forEach((metric, index) => {
    sql.append(SQL`(${metric.date}, ${metric.metric}, '', '', ${metric.value})`)
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

function buildMetricsQuery(period) {
  const metrics = [
    { metric: 'enxlogoall', label: 'ENX_LOGO_REQUESTS_ALL' },
    { metric: 'enxlogo200', label: 'ENX_LOGO_REQUESTS_200' },
    { metric: 'enxlogosettings', label: 'ENX_LOGO_REQUESTS_SETTINGS' },
    { metric: 'enxlogoenbuddy', label: 'ENX_LOGO_REQUESTS_ENBUDDY' },
    {
      metric: 'enxlogohealthenbuddy',
      label: 'ENX_LOGO_REQUESTS_HEALTHENBUDDY'
    }
  ]
  const metricsData = []

  metrics.forEach((m) => {
    metricsData.push({
      Id: `en_${m.metric}`,
      MetricStat: {
        Metric: {
          Namespace: 'ApiGateway',
          MetricName: m.metric
        },
        Period: period,
        Stat: 'Sum'
      },
      Label: `${m.label}`,
      ReturnData: true
    })
  })

  return metricsData
}

async function createENXLogoMetrics(client, event, hourlyBreakdown) {
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
  startDate.setDate(startDate.getDate() - 1)

  const endDate = new Date(startDate)
  endDate.setHours(23, 59, 59, 999)
  endDate.setDate(endDate.getDate() + 1)

  const params = {
    MetricDataQueries: buildMetricsQuery(hourlyBreakdown ? 3600 : 86400),
    StartTime: zonedTimeToUtc(startDate, timeZone),
    EndTime: zonedTimeToUtc(endDate, timeZone)
  }
  const logData = await new Promise((resolve, reject) => {
    cw.getMetricData(params, function (err, data) {
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

  if (!hourlyBreakdown) {
    results.forEach((response) => {
      dbMetrics.push({
        date: endDate,
        metric: response.Label,
        value:
          response.Values && response.Values.length > 0 ? response.Values[0] : 0
      })
      dbMetrics.push({
        date: startDate,
        metric: response.Label,
        value:
          response.Values && response.Values.length > 1 ? response.Values[1] : 0
      })
    })
    await storeENXLogoRequests(client, dbMetrics)
  } else {
    results.forEach((response) => {
      response.Timestamps.forEach((t, index) => {
        dbMetrics.push({
          date: utcToZonedTime(t, timeZone),
          metric: response.Label,
          value:
            response.Values && response.Values.length >= index
              ? response.Values[index]
              : 0
        })
      })
    })
    await storeENXHourlyData(client, dbMetrics)
  }
  console.log(
    'updated enx logo requests metrics',
    startDate,
    endDate,
    hourlyBreakdown
  )
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

exports.handler = async function (event) {
  const {
    codeLifetime,
    tokenLifetime,
    noticeLifetime
  } = await getExpiryConfig()

  await withDatabase(async (client) => {
    await createRegistrationMetrics(client)
    await removeExpiredCodes(client, codeLifetime)
    await removeExpiredTokens(client, tokenLifetime)
    await removeOldNoticesKeys(client, noticeLifetime)
    await createENXLogoMetrics(client, event, false)
    await createENXLogoMetrics(client, event, true)
    await createAPHLVerificationServerMetrics(client)
  })

  return true
}

runIfDev(exports.handler)
