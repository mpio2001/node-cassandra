const aws = require('aws-sdk');
const express = require('express');
const bodyParser = require('body-parser');
const cassandra = require('cassandra-driver');
const fs = require('fs');
const readline = require('readline');

const app = express();

const client = new cassandra.Client({
    contactPoints: ['34.200.231.63'],
    authProvider: new cassandra.auth.PlainTextAuthProvider('admin', 'adminpassword'),
    keyspace: 'node_angular'
});

client.connect((err, result) => {
    if (err) {
        console.log('Connection failed: ' + err);
    }
    else {
        console.log('Connected to the cassandra');
    }
});

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));

app.use((req, res, next) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PATCH, DELETE, PUT, OPTIONS');
    next();
});

//get posts
app.get('/api/posts', (req, res, next) => {
    const query = 'SELECT * FROM posts';
    const params = [];
    client.execute(query, params, (err, result) => {
        if (err) {
            res.status(404).send({ message: err });
        } else {
            res.status(200).json({
                message: 'Posts fetched successfully!',
                posts: result.rows
            });
        }

    });
});

//insert post
app.post('/api/posts', (req, res, next) => {
    const id = cassandra.types.uuid();
    const query = 'INSERT INTO posts(id, title, content) VALUES(?, ?, ?)';
    const params = [id, req.body.title, req.body.content];

    client.execute(query, params, (err, result) => {
        if (err) {
            res.status(404).send({ message: err });
        } else {
            res.status(201).json({
                message: 'Post added successfully',
                postId: id
            });
        }
    });
});

//insert post from s3
app.post('/api/s3posts', (req, res, next) => {

    // Set the region 
    aws.config.update({
        accessKeyId: "AKIAJDBAMNPBWCI42EKQ",
        secretAccessKey: "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",
        region: 'us-east-1'
    });

    // Create S3 service object
    const s3 = new aws.S3();
    const params = { Bucket: "hck2018" };
    const query = 'INSERT INTO posts(id, title, content) VALUES(?, ?, ?)';
    const queries = [];

    s3.listObjectsV2(params, (err, data) => {
        if (err) { // an error occurred
            console.log(err, err.stack);
        }
        else {  // successful response
            var promise = [];
            for (i = 0; i <= data.Contents.length - 1; i++) {
                var key = data.Contents[i].Key
                if (key.startsWith('source/') && key.endsWith('.csv')) {
                    promise.push(readFile(key));
                }

                if (i == (data.Contents.length - 1)) {
                    Promise.all(promise)
                        .then(writeToDb)
                        .then((success) => {
                            res.status(201).json(success);
                        }).catch((err) => {
                            res.status(404).send(err);
                        });
                }
            }
        }
    });

    var readFile = function (file) {
        return new Promise((resolve, reject) => {
            var rl = readline.createInterface({
                input: s3.getObject({ Bucket: params.Bucket, Key: file }).createReadStream()
            });

            rl.on('line', (line) => {
                const post = (line.toString()).split(',');
                queries.push({ query: query, params: [cassandra.types.uuid(), post[0], post[1]] });
            });

            rl.on('close', () => {
                resolve();
            });
        });
    }

    var writeToDb = function () {
        return new Promise((resolve, reject) => {
            client.batch(queries, { prepare: true }, (err) => {
                if (err) {
                    reject({ message: err });
                }
                else {
                    resolve({ message: 'Bulk Post added successfully' });
                }
            });
        });
    }
});

//delete post
app.delete('/api/posts/:id', (req, res, next) => {
    const query = "DELETE FROM posts WHERE id = ?";
    const params = [req.params.id];
    client.execute(query, params, (err, result) => {
        if (err) {
            res.status(404).send({ message: err });
        } else {
            res.status(200).json({
                message: 'Post deleted!'
            });
        }
    });
});

module.exports = app;