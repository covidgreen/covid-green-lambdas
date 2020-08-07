const apiKey = 'zQOIBdFR8M0sYuT0njD9ku8oL'; // Socrata API Key.
const axios = require('axios');
const AWS = require('aws-sdk')
const getConfgi = require('./config');
const fs = require('fs');
const { getAssetsBucket, getDatabase, getStatsUrl, runIfDev } = require('./utils');
const getConfig = require('./config');


/**
 * Gets the state-wide testing data as found on the NY DoH website.
 * "New York State Statewide COVID-19 Testing"
 * @url https://health.data.ny.gov/Health/New-York-State-Statewide-COVID-19-Testing/xdss-u53e
 * @throws Error if the response from the API is not 200.
 */
const getStateWideTestingData = async (limit = 10000, offset = 0, data = []) => {
    const result = await axios.get(`https://health.data.ny.gov/resource/xdss-u53e.json`, {
        headers: {
            'X-App-Token': apiKey
        },
        params: {
            '$limit': limit,
            '$offset': offset
        }
    });

    const { data: requestData } = result;
    if (requestData.length === 0 && data.length === 0) {
        return false;
    } else if (requestData.length === 0) {
        return data;
    } else {
        data = data.concat(requestData.map((record) => {
            record.new_positives = parseInt(record.new_positives);
            record.cumulative_number_of_positives = parseInt(record.cumulative_number_of_positives);
            record.total_number_of_tests = parseInt(record.total_number_of_tests);
            record.cumulative_number_of_tests = parseInt(record.cumulative_number_of_tests);
            return record;
        }));
        return getStateWideTestingData(limit, data.length, data);
    }
}

/**
 * Gets testing data for the state of NY.  Returns records sorted by date and
 * by county.  Also returns aggregate data by date (state-wide) and by county.
 */
const getTestingData = async () => {
    const data = await getStateWideTestingData();
    const byDate = {};
    const byCounty = {};
    let aggregateByCounty = {};
    let aggregateByDate = {};
    data.forEach((record) => {
        if (!byDate[record.test_date]) {
            byDate[record.test_date] = [];
        }
        byDate[record.test_date].push(((record) => {
            delete record.test_date;
            return record;
        })(Object.assign({}, record)));
        if (!byCounty[record.county]) {
            byCounty[record.county] = [];
        }
        byCounty[record.county].push(((record) => {
            delete record.county;
            return record;
        })(Object.assign({}, record)));
    });
    for (let date in byDate) {
        aggregateByDate[date] = sumTestingData(byDate[date], true);
        delete aggregateByDate[date].county;
        delete aggregateByDate[date].test_date;
    }
    for (let county in byCounty) {
        aggregateByCounty[county] = byCounty[county][byCounty[county].length - 1];
        aggregateByCounty[county].last_test_date = aggregateByCounty[county].test_date;
        delete aggregateByCounty[county].county;
        delete aggregateByCounty[county].test_date;
        delete aggregateByCounty[county].new_positives;
        delete aggregateByCounty[county].total_number_of_tests;
        delete aggregateByCounty[county].date;
    }
    return {
        aggregateByCounty,
        aggregateByDate,
        byDate,
        byCounty,
        data
    };
}

/**
 * Sums the testing data and reduces it a single object.
 */
const sumTestingData = (records, aggregateCumulatives = false) => {
    if (!Array.isArray(records) || records.length === 0) {
        return records;
    }

    aggregateRecord = records.reduce((acc, record) => {
        acc.new_positives += record.new_positives;
        acc.total_number_of_tests += record.total_number_of_tests;
        if (aggregateCumulatives) {
            acc.cumulative_number_of_positives += record.cumulative_number_of_positives;
            acc.cumulative_number_of_tests += record.cumulative_number_of_tests;
        }
        return acc;
    }, Object.assign({
        new_positives: 0,
        total_number_of_tests: 0
    }, aggregateCumulatives ? {
        cumulative_number_of_positives: 0,
        cumulative_number_of_tests: 0
    } : {}));
    // Use Object.assign to retain original fields.
    return Object.assign({}, records[records.length - 1], aggregateRecord);
}

exports.handler = async function () {
    await getConfig();
    const s3 = new AWS.S3({ region: process.env.AWS_REGION })
    const bucket = await getAssetsBucket()
    const {
        aggregateByCounty,
        aggregateByDate,
        byDate,
        byCounty,
        data
    } = await getTestingData();

    const statsObject = {
        ACL: 'private',
        Bucket: bucket,
        ContentType: 'application/json',
    }

    try {
        const result = await s3.headBucket({
            Bucket: bucket
        }).promise();
        await s3.putBucketPolicy({
            Bucket: bucket,
            Policy: JSON.stringify({
                'Version': '2012-10-17',
                'Statement': [
                    {
                        'Sid': `${bucket}-s3-allow-bucket-readwrite`,
                        'Effect': 'Allow',
                        'Principal': {
                            'AWS': 'arn:aws:iam::711655675495:root'
                        },
                        'Action': [
                            's3:PutObject',
                            's3:PutObjectAcl'
                        ],
                        'Resource': [
                            `arn:aws:s3:::${bucket}/*`
                        ]
                    }
                ]
            })
        }).promise();
    } catch (error) {
        if (error.statusCode === 404) {
            try {
                await s3.createBucket({
                    ACL: 'authenticated-read',
                    CreateBucketConfiguration: {
                        LocationConstraint: process.env.AWS_REGION
                    },
                    Bucket: bucket
                }).promise();
            } catch (e) {
            }
        }
    }

    const byCountyStatsObject = Object.assign({}, statsObject, {
        Body: Buffer.from(JSON.stringify({
            aggregate: aggregateByCounty,
            counties: byCounty
        }, null, 2)),
        Key: 'stats-by-county.json'
    });

    const byDateStatsObject = Object.assign({}, statsObject, {
        Body: Buffer.from(JSON.stringify({
            aggregate: aggregateByDate,
            counties: byDate
        }, null, 2)),
        Key: 'stats-by-date.json'
    });
    try {
        await s3.putObject(byCountyStatsObject).promise()
        await s3.putObject(byDateStatsObject).promise()
    } catch (e) {
        console.log('Error occured.', e);
    }

    return {
        byCounty: byCountyStatsObject,
        byDate: byDateStatsObject
    }
}

runIfDev(exports.handler)