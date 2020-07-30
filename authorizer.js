const { isAuthorized, runIfDev } = require('./utils')

exports.handler = async function (event) {
  const secret = process.env.JWT_SECRET
  if (!secret) {
    console.log('Error acquiring secret from env')
    throw 'JWTSecretError'
  }

  if (!isAuthorized(event.authorizationToken, secret)) {
    throw 'Unauthorized'
  }

  return {
    principalId: event.authorizationToken,
    policyDocument: {
      Version: '2012-10-17',
      Statement: [
        {
          Action: 'execute-api:Invoke',
          Effect: 'Allow',
          Resource: 'arn:aws:execute-api:*'
        }
      ]
    }
  }
}

runIfDev(exports.handler)
