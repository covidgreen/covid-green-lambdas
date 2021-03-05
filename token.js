const jwt = require('jsonwebtoken')
const SQL = require('@nearform/sql')
const { withDatabase, getJwtSecret, runIfDev } = require('./utils')

exports.handler = async function (event) {
  const { description, type } = event

  if (!description) {
    throw new Error('Description is missing')
  }

  const sql = SQL`
    INSERT INTO tokens (type, description)
    VALUES (${type || 'push'}, ${description})
    RETURNING id`

  const secret = await getJwtSecret()

  return await withDatabase(async (client) => {
    const { rowCount, rows } = await client.query(sql)

    if (rowCount === 0) {
      throw new Error('Unable to create token')
    }

    const [{ id }] = rows

    return jwt.sign({ id }, secret)
  })
}

runIfDev(exports.handler)
