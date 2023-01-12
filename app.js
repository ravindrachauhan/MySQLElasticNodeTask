const mysql = require('mysql')


const connection = mysql.createConnection({
    host: 'localhost',
    user:'root',
    password: 'password',
    database:'ravidb'
})


connection.connect(err => {
    if(err) throw err
    console.log('Database connected')
})





// const express = require('express')
// const app = express()
// port = 4000

// app.listen(port,()=>{
//     console.log(`the server is running on port, ${port}`)
// })    
        
const elasticsearch = require('elasticsearch');
const { ELASTIC_SEARCH_URL='https://slashAdmin:FlawedByDesign@1612$@elastic-50-uat.slashrtc.in/elastic'} = process.env;
let client = null;


const connect = async () => {
    client = new elasticsearch.Client({
      host: ELASTIC_SEARCH_URL,
      log: { type: 'stdio', levels: [] }
    });
    return client;
  };
  
  
  
  const ping = async () => {
    let attempts = 0;
    const pinger = ({ resolve, reject }) => {
      attempts += 1;
      client
        .ping({ requestTimeout: 30000 })
        .then(() => {
          console.log('Elasticsearch server available');
          resolve(true);
        })
        .catch(() => {
          if (attempts > 100) reject(new Error('Elasticsearch failed to ping'));
          console.log('Waiting for elasticsearch server...');
          setTimeout(() => {
            pinger({ resolve, reject });
          }, 1000);
        });
    };
  
    return new Promise((resolve, reject) => {
      pinger({ resolve, reject });
    });
  };
  
  const con=async()=>{
    try{
      await connect();
      await ping();
    }catch(error){
      console.log(error)
    }}
  
  con()
  let cdrid =  "26fe698a-c50d-4543-ae-a20c45a46919"
  async function run() {
    const response = await client.search({
      index: 'deliveriesdevlogger2022-11-04',
      body: {
        query: {
          bool: {
            must: [
              {
                match: {
                  "callinfo.agentLegUuid.keyword": cdrid,
                }
              }
            ]
          }
        }
      }
    })
  
    console.log(response.hits.hits)
  }

  run()

  const updation2 = async (row) => {const update = {
    script: {
      source: 
             `ctx._source.callinfo.callTime.talkTime = ${row.agent_talktime_sec}`,

    },
    query: {
      bool: {   
        must: {
          match: {
            "callinfo.agentLegUuid.keyword":`${row.cdrid}`,
          },
        },
      },
    },
  };
  client
    .updateByQuery({
      index: "deliveriesdevlogger2022-11-04",
      body: update,
    })
    .then(
      (res) => {
        console.log("Success", res);
      },
      (err) => {
        console.log("Error", err);
      }
    );
  }

    // const stream = connection.query(
    //     'SELECT * FROM disposecall'
    // ).stream();
    
    // stream.on('data', (row) => {
    //     // Process the row
    //     console.log(row);
    // });
    
    // stream.on('end', () => {
    //     // All rows have been received
    //     connection.end();
    // });

    const stream = connection.query(
        'SELECT * FROM disposecall'
    ).stream();
    
    stream.on('data', async(row) => {
        // Process the row
        updation2(row)
        console.log(row)
    });
    
    stream.on('end', () => {
        // All rows have been received
        connection.end();
    });