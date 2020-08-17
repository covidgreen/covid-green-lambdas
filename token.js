const jwt = require('jsonwebtoken')
const SQL = require('@nearform/sql')
const { getDatabase, getJwtSecret, runIfDev } = require('./utils')

exports.handler = async function(event) {
  const { description, type } = event

  if (!description) {
    throw new Error('Description is missing')
  }

  const sql = SQL`
    INSERT INTO tokens (type, description)
    VALUES (${type || 'push'}, ${description})
    RETURNING id`

  const secret = await getJwtSecret()
  const client = await getDatabase()
  const { rowCount, rows } = await client.query(sql)

  if (rowCount === 0) {
    throw new Error('Unable to create token')
  }

  const [{ id }] = rows

  return jwt.sign({ id }, secret, { expiresIn: '1y' })
}

runIfDev(exports.handler)
