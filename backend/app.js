const aws = require('aws-sdk');
const express = require('express');
const bodyParser = require('body-parser');
const cassandra = require('cassandra-driver');
const fs = require('fs');
const readline = require('readline');

const app = express();

const client = new cassandra.Client({
    contactPoints: [''],
    authProvider: new cassandra.auth.PlainTextAuthProvider('', ''),
    keyspace: 'hck_2018'
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
    aws.config.update({ region: 'us-east-1' });

    // Create S3 service object
    const s3 = new aws.S3();
    const bucket = req.body.bucket;
    const file = req.body.key;
    const query = 'INSERT INTO posts(id, title, content) VALUES(?, ?, ?)';
    const queries = [];

    var rl = readline.createInterface({
        input: s3.getObject({ Bucket: bucket, Key: file }).createReadStream()
    });

    rl.on('line', (line) => {
        const post = (line.toString()).split(',');
        queries.push({ query: query, params: [cassandra.types.uuid(), post[0], post[1]] });
    });

    rl.on('close', () => {
        writeToDb();
    });

    var writeToDb = function () {
        return new Promise((resolve, reject) => {
            client.batch(queries, { prepare: true }, (err) => {
                if (err) {
                    res.status(404).send(err);
                }
                else {
                    res.status(201).json({ message: 'Bulk Post added successfully' });
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