const AWS = require('aws-sdk')
const SQL = require('@nearform/sql')
const { createTransport } = require('nodemailer')
const { format } = require('date-fns')
const { getAlertConfig, withDatabase, runIfDev } = require('./utils')

async function getVenueConfig(client, id) {
  const sql = SQL`
    SELECT
      COALESCE(v.threshold_count, t.threshold_count) AS "thresholdCount",
      COALESCE(v.threshold_duration, t.threshold_duration) AS "thresholdDuration"
    FROM qr_code v
    LEFT JOIN venue_types t ON t.id = v.venue_type
    WHERE v.id = ${id}`

  const { rows } = await client.query(sql)
  const [{ thresholdCount, thresholdDuration }] = rows

  return { thresholdCount, thresholdDuration }
}

async function getAlerts(client, id, date, thresholdCount, thresholdDuration) {
  const sql = SQL`
    WITH
      dates AS (
        SELECT GENERATE_SERIES(
          ${date}::TIMESTAMPTZ - ${`${thresholdDuration} hours`}::INTERVAL,
          ${date}::TIMESTAMPTZ,
          '1 hours'::INTERVAL
        ) AS start_date
      ),
      ranges AS (
        SELECT
          start_date,
          start_date + ${`${thresholdDuration} hours`}::INTERVAL AS end_date
        FROM dates
      ),
      results AS (
        SELECT
          start_date,
          end_date,
          (
            SELECT COUNT(*)
            FROM venue_check_ins
            WHERE checked_in_at BETWEEN start_date AND end_date
            AND venue_id = ${id}
          ) AS check_ins
        FROM ranges
      )
    SELECT
      start_date AS "startDate",
      end_date AS "endDate",
      check_ins AS "checkIns"
    FROM results
    WHERE check_ins >= ${thresholdCount}`

  const { rows } = await client.query(sql)

  return rows
}

exports.handler = async function(event) {
  const ses = new AWS.SES({ region: process.env.AWS_REGION })
  const transport = createTransport({ SES: ses })
  const { emailAddress, sender } = await getAlertConfig()

  console.log(`processing ${event.Records.length} records`)

  await withDatabase(async client => {
    for (const record of event.Records) {
      const { id, date } = JSON.parse(record.body)
      const { thresholdCount, thresholdDuration } = await getVenueConfig(
        client,
        id
      )

      if (thresholdCount && thresholdDuration) {
        console.log(
          `checking for alerts for venue ${id} on ${date} (${thresholdCount} uploads in ${thresholdDuration} hours)`
        )

        const alerts = await getAlerts(
          client,
          id,
          date,
          thresholdCount,
          thresholdDuration
        )

        if (alerts.length > 0) {
          console.log(`found ${alerts.length} periods exceeding threshold`)

          const venues = alerts
            .map(
              ({ startDate, endDate, checkIns }) =>
                `${format(startDate, 'yyyy-MM-dd HH:mm')} to ${format(
                  endDate,
                  'yyyy-MM-dd HH:mm'
                )}: ${checkIns} check-ins`
            )
            .join('\n')

          await transport.sendMail({
            from: sender,
            subject: `Venue ${id} has triggered an alert`,
            text: `Venue ${id} triggered alerts: \n\n${venues}`,
            to: emailAddress
          })
        }
      }
    }
  })

  return true
}

runIfDev(exports.handler)
