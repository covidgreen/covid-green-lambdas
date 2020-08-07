const envSchema = require('env-schema')
const S = require('fluent-schema')
const AWS = require('aws-sdk')
const { version } = require('./package.json')

async function getConfig() {
    const env = envSchema({
        dotenv: true,
        schema: S.object()
            .prop('CONFIG_VAR_PREFIX', S.string())
            .prop('NODE_ENV', S.string())
            .prop('API_HOST', S.string())
            .prop('API_PORT', S.string())
            .prop('CORS_ORIGIN', S.string())
            .prop('DB_HOST', S.string())
            .prop('DB_READ_HOST', S.string())
            .prop('DB_PORT', S.string())
            .prop('DB_USER', S.string())
            .prop('DB_PASSWORD', S.string())
            .prop('DB_DATABASE', S.string())
            .prop('DB_SSL', S.boolean())
            .prop(
                'LOG_LEVEL',
                S.string()
                    .enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent'])
                    .default('info')
            )
            .prop('AWS_ACCESS_KEY_ID', S.string())
            .prop('AWS_SECRET_ACCESS_KEY', S.string())
            .prop('AWS_REGION', S.string())
            .prop('DEFAULT_REGION', S.string())
            .prop('ENCRYPT_KEY', S.string())
            .prop('EXPOSURE_LIMIT', S.number())
            .prop('JWT_SECRET', S.string())
            .prop('REFRESH_TOKEN_EXPIRY', S.string())
            .prop('TOKEN_LIFETIME_MINS', S.number())
            .prop('CODE_LIFETIME_MINS', S.number())
            .prop('UPLOAD_TOKEN_LIFETIME_MINS', S.number())
            .prop('VERIFY_RATE_LIMIT_SECS', S.number())
            .prop('DEVICE_CHECK_KEY_ID', S.string())
            .prop('DEVICE_CHECK_KEY', S.string())
            .prop('DEVICE_CHECK_TEAM_ID', S.string())
            .prop('DEVICE_CHECK_PACKAGE_NAME', S.string())
            .prop('DEVICE_CHECK_PACKAGE_DIGEST', S.string())
            .prop('DEVICE_CHECK_CERTIFICATE_DIGEST', S.string())
            .prop('DEVICE_CHECK_ROOT_CA', S.string())
            .prop('DEVICE_CHECK_TIME_DIFF_THRESHOLD_MINS', S.number())
            .prop('CALLBACK_QUEUE_URL', S.string())
            .prop('ASSETS_BUCKET', S.string())
            .prop('METRICS_CONFIG', S.string())
            .prop('ENABLE_CALLBACK', S.boolean())
            .prop('ENABLE_CHECK_IN', S.boolean())
            .prop('ENABLE_METRICS', S.boolean())
            .prop('DEFAULT_REGION', S.string())
    })

    Object.assign(process.env, env);
    return process.env;
}

module.exports = getConfig
