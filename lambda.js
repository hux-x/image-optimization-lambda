import { S3Client, GetObjectCommand, PutObjectCommand, HeadObjectCommand } from "@aws-sdk/client-s3";
import sharp from "sharp";
import "dotenv/config";

// S3 client (can use IAM role or env vars)
const s3Client = new S3Client({
  region: "us-east-1",
  credentials:  {
        accessKeyId: process.env.AWS_ACCESS_KEY,
        secretAccessKey: process.env.AWS_SECRET_KEY ,
      }
  
});

// Lambda handler triggered by SQS
export const handler = async (event) => {
  console.log(`Received ${event.Records.length} message(s)`);

  for (const sqsRecord of event.Records) {
    try {
      // S3 event is inside SQS message body
      const record = JSON.parse(sqsRecord.body).Records[0];
      const bucket = record.s3.bucket.name;
      const key = decodeURIComponent(record.s3.object.key.replace(/\+/g, " "));

      // Check if already optimized
      const head = await s3Client.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
      if (head.Metadata?.optimized === "true") {
        console.log(`Skipping already optimized image: ${key}`);
        continue;
      }

      console.log(`Processing image: ${bucket}/${key}`);

      // Download image from S3
      const object = await s3Client.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
      const chunks = [];
      for await (const chunk of object.Body) chunks.push(chunk);
      const buffer = Buffer.concat(chunks);

      // Optimize image using Sharp
      const optimizedBuffer = await sharp(buffer)
        .resize({ width: 1024 })
        .webp({ quality: 80 })
        .toBuffer();

      // Upload optimized image back to S3 with metadata
      await s3Client.send(
        new PutObjectCommand({
          Bucket: bucket,
          Key: key,
          Body: optimizedBuffer,
          ContentType: "image/webp",
          Metadata: { optimized: "true" }, // prevents loop
        })
      );

      console.log(`Image optimized successfully: ${key}`);
    } catch (err) {
      console.error("Error processing image:", err);
      throw err; // Lambda will retry the message if using SQS trigger
    }
  }

  return {
    statusCode: 200,
    body: JSON.stringify({ message: "Batch processed successfully" }),
  };
};
