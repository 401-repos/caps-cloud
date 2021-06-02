'use strict';

const AWS = require('aws-sdk');
require('dotenv').config();
const {
    Consumer
} = require('sqs-consumer');
const {
    Producer
} = require('sqs-producer');
const AWS_REGION = process.env.AWS_REGION;
const AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID;
const AWS_SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY;
const AWS_SESSION_TOKEN = process.env.AWS_SESSION_TOKEN;
const VENDOR_SQS_URL = process.env.VENDOR_SQS_URL;
const PACKAGES_SQS_UR = process.env.PACKAGES_SQS_UR;
AWS.config.update({
    region: AWS_REGION,
    accessKeyId: AWS_ACCESS_KEY_ID,
    secretAccessKey: AWS_SECRET_ACCESS_KEY,
    sessionToken: AWS_SESSION_TOKEN
});
const producer = Producer.create({
    queueUrl: VENDOR_SQS_URL,
    region: AWS_REGION,
    accessKeyId: AWS_ACCESS_KEY_ID,
    secretAccessKey: AWS_SECRET_ACCESS_KEY,
    sessionToken: AWS_SESSION_TOKEN
});
const app = Consumer.create({
    queueUrl: PACKAGES_SQS_UR,
    handleMessage: orderHandler,
});
let orderNum = 0;
function orderHandler(message) {
    let parsedBody = JSON.parse(message.Body);
    let parsedOrder = JSON.parse(parsedBody.Message);
    console.log("New order is assigned:", parsedOrder);
    setTimeout(async () => {
        try {
            let id = `orderNum${orderNum++}`;
            await producer.send([parsedOrder])
        }catch(err){
            console.log( "Error in driver send", err.message)
        }
    }, 5000);
}

app.on('error', (err) => {
    console.error( "Error in driver read",err.message);
});

app.on('processing_error', (err) => {
    console.error(err.message);
});

app.start();