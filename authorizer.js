const { isAuthorized, runIfDev } = require('./utils')

exports.handler = async function (event) {
  const secret = process.env.JWT_SECRET

  if (!secret) {
    console.log('Error acquiring secret from env')

    // We're ignoring this lint error because the Lambdas are configured to respond
    // to a thrown string 'JWTSecretError'. For the moment we'll leave this as is
    // but long-term we should reconfigure the lambda to respond to a thrown Error
    // instead of an explicit literal string
    // TODO: reconfigure lambda to respond to a thrown Error instead of literal 'JWTSecretError'

    // eslint-disable-next-line no-throw-literal
    throw 'JWTSecretError'
  }

  if (!isAuthorized(event.authorizationToken, secret)) {
    // We're ignoring this lint error because the Lambdas are configured to respond
    // to a thrown string 'Unauthorized'. For the moment we'll leave this as is
    // but long-term we should reconfigure the lambda to respond to a thrown Error
    // instead of an explicit literal string. The configuration
    // TODO: reconfigure lambda to respond to a thrown Error instead of literal 'Unauthorized'

    // eslint-disable-next-line no-throw-literal
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
