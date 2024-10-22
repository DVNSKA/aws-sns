const AWS = require('aws-sdk');
const XLSX = require('xlsx');

const s3 = new AWS.S3();
const ses = new AWS.SES();

exports.handler = async (event) => {
    const bucket = event.Records[0].s3.bucket.name;
    const key = event.Records[0].s3.object.key;

    try {
        // Read the Excel file from S3
        const params = { Bucket: bucket, Key: key };
        const { Body } = await s3.getObject(params).promise();
        
        // Parse the Excel file
        const workbook = XLSX.read(Body, { type: 'buffer' });
        const sheetName = workbook.SheetNames[0];
        const sheet = workbook.Sheets[sheetName];

        // Convert to JSON
        let data = XLSX.utils.sheet_to_json(sheet);

        // Filter the data
        const filteredData = data.filter(row => 
            row['__EMPTY'] === 'Non-Production' && row['__EMPTY_6'] === 'OVER_PROVISIONED'
        );

        // If filteredData is empty, handle the case
        if (filteredData.length === 0) {
            return {
                statusCode: 200,
                body: JSON.stringify('No matching data found.'),
            };
        }

        // Sort the data based on column 'n' (replace 'n' with the actual key)
        // Sort the data based on column '__EMPTY_12' in descending order
filteredData.sort((a, b) => b['__EMPTY_12'] - a['__EMPTY_12']); // For numeric values


        // Get the top row after sorting
        const topRow = filteredData[0];

        // Extract email address and body content
        const email = topRow['__EMPTY_10'];
        const bodyContent = topRow['__EMPTY_3'];

        // Send email via SES
        const emailParams = {
            Source: 'dundivenkatanagasaikondalaabhiram.pabbisettynagaraju@hidglobal.com', // Replace with your verified email
            Destination: {
                ToAddresses: [email]
            },
            Message: {
                Subject: { Data: 'Top Row Notification' },
                Body: {
                    Text: { Data: `Top Row B value: ${bodyContent}` }
                }
            }
        };

        await ses.sendEmail(emailParams).promise();

        // Create a new workbook and add the filtered data
        const newSheet = XLSX.utils.json_to_sheet(filteredData);
        const newWorkbook = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(newWorkbook, newSheet, 'Filtered Data');

        // Write the new workbook to a buffer
        const newBuffer = XLSX.write(newWorkbook, { bookType: 'xlsx', type: 'buffer' });

        // Upload the new Excel file back to S3
        const newKey = `sorted_${key}`;
        const uploadParams = {
            Bucket: bucket,
            Key: newKey,
            Body: newBuffer,
            ContentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        };

        await s3.putObject(uploadParams).promise();

        return {
            statusCode: 200,
            body: JSON.stringify('File processed and email sent successfully!'),
        };
    } catch (error) {
        console.error(error);
        return {
            statusCode: 500,
            body: JSON.stringify('Error processing file: ' + error.message),
        };
    }
};
