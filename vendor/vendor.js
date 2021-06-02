'use strict';

require('dotenv').config();
const faker = require('faker');
const AWS = require('aws-sdk');
const {
    Consumer
} = require('sqs-consumer')
const AWS_REGION = process.env.AWS_REGION;
const AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID;
const AWS_SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY;
const AWS_SESSION_TOKEN = process.env.AWS_SESSION_TOKEN;
const VENDOR_SQS_URL = process.env.VENDOR_SQS_URL;
const SNS_ARN = process.env.SNS_ARN;
AWS.config.update({
    region: AWS_REGION,
    accessKeyId: AWS_ACCESS_KEY_ID,
    secretAccessKey: AWS_SECRET_ACCESS_KEY,
    sessionToken: AWS_SESSION_TOKEN
});
const sns = new AWS.SNS();


const topic = SNS_ARN;

setInterval(() => {

    let newOrder = {
        orderId: faker.datatype.uuid(),
        customer: faker.name.findName(),
        vendorId: VENDOR_SQS_URL
    }
    let payload = {
        Message: JSON.stringify(newOrder),
        TopicArn: topic
    }

    sns.publish(payload).promise()
        .then(data => {
            console.log(`Your message with the id ${newOrder.orderId} has been successfully published`, data , '\n++++++++++++++++++++++++++++++');
        }).catch(err => {
            console.log("Error when vendor publish:", err.message);
        });

}, 5000);

const app = Consumer.create({
    queueUrl: VENDOR_SQS_URL,
    handleMessage: messageHandler,
});

function messageHandler(message){
    let parsedBody = JSON.parse(message.Body);
    // let parsedMessage = JSON.parse(parsedBody.Message);
    console.log("New message is Received:", message.Body, '\n/////////////////////////////////');
}

app.on('error', (err) => {
    console.log(err.message);
});
app.on('processing_error', (err) => {
    console.error(err.message);
});
app.start();