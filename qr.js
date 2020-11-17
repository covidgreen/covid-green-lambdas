const AWS = require('aws-sdk')
const PDFDocument = require('pdfkit')
const QRCode = require('qrcode')
const { createTransport } = require('nodemailer')
const { runIfDev, getQrConfig } = require('./utils')

function createPDFContent({ qrCode, name, location }) {
  return new Promise(resolve => {
    const doc = new PDFDocument({ size: 'A4' })
    const buffers = []

    doc.on('data', data => buffers.push(data))
    doc.on('end', () => resolve(Buffer.concat(buffers)))

    doc.image('./pdf-assets/header.png', 0, 0, { width: doc.page.width })

    doc
      .font('Helvetica-Bold', 18)
      .text(name, (doc.page.width - doc.page.width) / 2, 305, {
        align: 'center',
        width: doc.page.width
      })

    doc.image(qrCode, (doc.page.width - 400) / 2, 330, { width: 400 })

    doc
      .font('Helvetica-Bold', 18)
      .text(location, (doc.page.width - doc.page.width) / 2, 740, {
        align: 'center',
        width: doc.page.width
      })

    doc.image(
      './pdf-assets/footer.png',
      (doc.page.width - (doc.page.width - 50)) / 2,
      doc.page.height - 75,
      {
        width: doc.page.width - 50
      }
    )

    doc.end()
  })
}

exports.handler = async function(event) {
  const s3 = new AWS.S3({ region: process.env.AWS_REGION })
  const ses = new AWS.SES({ region: process.env.AWS_REGION })
  const transport = createTransport({ SES: ses })
  const { bucketName, appUrl, sender } = await getQrConfig()

  console.log(`processing ${event.Records.length} records`)

  for (const record of event.Records) {
    const { emailAddress, id, location, name, token } = JSON.parse(record.body)

    console.log(`generating poster ${id}`)

    const data = await createPDFContent({
      qrCode: await QRCode.toDataURL(`${appUrl}?content=${token}`),
      name,
      location
    })

    console.log(`writing to ${bucketName}`)

    const object = {
      ACL: 'private',
      Body: data,
      Bucket: bucketName,
      ContentType: 'application/pdf',
      Key: `${id}.pdf`
    }

    await s3.putObject(object).promise()

    console.log(`sending email`)

    await transport.sendMail({
      from: sender,
      subject: 'Your QR poster is attached',
      text: 'Your QR posted is attached',
      to: emailAddress,
      attachments: [
        {
          filename: 'qr.pdf',
          content: data
        }
      ]
    })

    console.log(`email sent`)
  }

  return true
}

runIfDev(exports.handler)
