const { runIfDev } = require('./utils')

exports.handler = (event, context, callback) => {
  const request = event.Records[0].cf.request // extract the request object
  request.uri = request.uri.replace(/\/api\/data\/exposures\//, '') // modify the URI
  return callback(null, request) // return control to CloudFront
}

runIfDev(exports.handler)
