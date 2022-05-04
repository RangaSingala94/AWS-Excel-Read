const AWS = require('aws-sdk');
const xlsx = require('node-xlsx');
//const xlsx = require('xlsx');
//const fs  = require('fs');
const s3 = new AWS.S3({apiVersion: "2006-03-01"});
AWS.config.update({region: 'us-east-1'});
// TODO implement
const params = {Bucket: 'mynew-excel-read', Key: 'NewExternal.xlsx'};
var sqs = new AWS.SQS({apiVersion: '2012-11-05'});
const accountId = '240717474084';
const queueName = 'MyLatestDataRead.fifo';
const sqsUrl = `https://sqs.us-east-1.amazonaws.com/${accountId}/${queueName}`;

exports.handler = async (event, context, callback) => {
    try {
        const stream = s3.getObject(params).createReadStream();
        const buffer = await streamToChunk(stream);
        const workbook = xlsx.parse(buffer);
        let [header, ...dataRows] = workbook[0].data;
        rows = [];
        let i = 1;
        for(const row of dataRows) {
            if(row.length > 0) {
                const data = {
                    id: row[0],
                    day: row[1],
                    type: row[2],
                    value1: row[3],
                    value2: row[4],
                    value3: row[5],
                    name: row[6]
                }
                await sendInSqs(data, i++);
                rows.push(data);
            }
        }
        
        return JSON.stringify(rows);
        //const workbook = xlsx.read(buffer, {type: buffer});
        //const sheetName = workbook.SheetNames[0];
        //callback({status: 200, 'body': xlsx.utils.sheet_to_json(workbook.Sheets[sheetName])});
    } catch(err) {
        console.log(err);
        callback({status: 400, 'body':JSON.stringify(err)});
        throw new Error(err);
    }
};

async function streamToChunk(stream) {
    const chunks = [];
    return new Promise((resolve, reject) => {
        
      stream.on('data', (chunk) => chunks.push(chunk));
      stream.on('error', (err) => reject(err));
      stream.on('end', () => resolve(Buffer.concat(chunks)));
    })
}

async function sendInSqs(rows, i) {
    const date = new Date().getTime();
    const params = {
        MessageBody: JSON.stringify(rows),
        MessageDeduplicationId: `row-id-${date}-${i}`,
        MessageGroupId: "Group1",
        QueueUrl: sqsUrl
      };
    return sqs.sendMessage(params).promise();
}
  
  