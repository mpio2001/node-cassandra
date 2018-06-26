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