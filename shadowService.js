const mqtt = require('mqtt')
const redis = require('redis')
const {getTopics, removeNulledProps, errObj} = require('./util')
const _ = require('lodash')

let client
let redisClient
let log

module.exports = (options, _log) => {
  client = mqtt.connect(`ws://${options.host}:${options.httpPort}/mqqt`)
  redisClient = redis.createClient()
  log = _log

  client.on('connect', () => {
    client.subscribe('$aws/things/+/shadow/update')
    client.subscribe('$aws/things/+/shadow/delete')
    client.subscribe('$aws/things/+/shadow/get')
    log('Shadow service connected to IOT broker.')
  })

  client.on('message', (topic, message) => {
    const serialNumber = topic.split('/')[2]
    const action = topic.split('/')[4]
    switch (action) {
      case 'update':
        handleUpdateTopic(serialNumber, JSON.parse(message.toString()))
        break
      case 'get':
        log(`Retrieving shadow for '${serialNumber}'`)
        handleGetTopic(serialNumber)
        break
      case 'delete':
        log(`Deleting shadow for '${serialNumber}'`)
        handleDeleteTopic(serialNumber)
    }
  })

  return {client, redisClient}
}

const handleUpdateTopic = (serialNumber, shadow) => {
  redisClient.get(serialNumber, (err, result) => {
    if (err) {
      return publishError(err)
    }
    /*
    if (shadow.state.desired) {
      if (!result) {
        return publishError('Can\'t update desired state for non existing thing.')
      }
      return updateShadowDesired(serialNumber, JSON.parse(result), shadow)
    } else {
     */
      return updateShadow(serialNumber, (result ? JSON.parse(result) : null), shadow)
    /*
    }
     */
  })
}

function removeNull(obj){
  var isArray = obj instanceof Array;
  for (var k in obj){
    if (obj[k]===null) isArray ? obj.splice(k,1) : delete obj[k];
    else if (typeof obj[k]=="object") removeNull(obj[k]);
  }
  return obj
}

const fixDelta = (obj) => {
  if (!obj.state.desired || !obj.state.reported)
    return obj

  const fixup = (desired, reported) => {
    Object.keys(desired).forEach((key) => {
      if (reported[key] === undefined)
        return;
      if (typeof desired[key] !== typeof reported[key] || Array.isArray(desired[key]) != Array.isArray(reported[key]))
        return;
      if (typeof desired[key] === 'object') {
        fixup(desired[key], reported[key]);
        if (Object.keys(desired[key]).length == 0)
          desired[key] = null;
      }
      let dv = desired[key];
      let rv = reported[key];
      if (Array.isArray(dv)) {
        dv = JSON.stringify(dv);
        rv = JSON.stringify(rv);
      }
      if (dv === rv) {
        desired[key] = null;
      }
    })
  }
  fixup(obj.state.desired, obj.state.reported)
  return obj
}
const updateShadow = (serialNumber, prevShadow, shadow) => {
  log(`Updating shadow reported state for '${serialNumber}'`)
  const newShadow = removeNull(fixDelta(_.merge(prevShadow, shadow)))
  updateThingDb(serialNumber, newShadow).then((version) => {
    publishAccepted(shadow, serialNumber, version)
    publishDelta(newShadow, serialNumber, version)
    publishDocuments(prevShadow, serialNumber, newShadow, version)
  }).catch((err) => publishError(err, serialNumber))
}

/*
const updateShadowDesired = (serialNumber, prevShadow, desiredShadow) => {
  log(`Updating shadow desired state for '${serialNumber}'`)
  const newShadow = removeNull(_.merge(prevShadow, desiredShadow))
  updateThingDb(serialNumber, newShadow).then((version) => {
    publishAccepted(desiredShadow, serialNumber, version)
    publishDelta(newShadow, serialNumber, version)
    publishDocuments(prevShadow, serialNumber, newShadow, version)
  }).catch((err) => publishError(err, serialNumber))
}
*/

const publishAccepted = (shadow, serialNumber, version) => {
  client.publish(getTopics(serialNumber).updateAccepted, JSON.stringify(Object.assign(shadow, {
    timestamp: new Date().getTime(),
    version
  })))
}

const publishDelta = (shadow, serialNumber, version) => {
  client.publish(getTopics(serialNumber).updateDelta, JSON.stringify({
    timestamp: new Date().getTime(),
    version,
    state: shadow.state.desired
  }))
}

const publishDocuments = (prevShadow, serialNumber, currentShadow, version) => {
  client.publish(getTopics(serialNumber).updateDocuments, JSON.stringify({
    previous: Object.assign({}, prevShadow, {version: --version}),
    current: Object.assign({}, currentShadow, {version}),
    timestamp: new Date().getTime()
  }))
}

const publishError = (err, serialNumber) => {
  client.publish(getTopics(serialNumber).updateRejected, JSON.stringify(errObj(err.toString())))
}

const updateThingDb = (serialNumber, shadow, isDelete) => {
  return new Promise((resolve, reject) => {
    const newShadow = removeNulledProps(shadow)
    redisClient.set(serialNumber, JSON.stringify(newShadow), (err) => {
      if (err) {
        return reject(err)
      }

      if (isDelete) {
        redisClient.set(`${serialNumber}Version`, '1', (err, version) => {
          if (err) {
            return reject(err)
          }
          return resolve(version)
        })
      } else {
        redisClient.incr(`${serialNumber}Version`, (err, version) => {
          if (err) {
            return reject(err)
          }
          return resolve(version)
        })
      }
    })
  })
}

const handleGetTopic = (serialNumber) => {
  redisClient.get(serialNumber, (err, result) => {
    if (err || !result) {
      return client.publish(getTopics(serialNumber).getRejected, JSON.stringify(errObj(err ? err.toString() : 'No shadow stored for that serial number')))
    }
    const shadow = JSON.parse(result)
    redisClient.get(`${serialNumber}Version`, (err, version) => {
      return client.publish(getTopics(serialNumber).getAccepted, JSON.stringify(Object.assign({}, shadow, {
        version,
        timestamp: new Date().getTime()
      })))
    })
  })
}

const handleDeleteTopic = (serialNumber) => {
  updateThingDb(serialNumber, null, true).then((version) => {
    client.publish(getTopics(serialNumber).deleteAccepted, JSON.stringify({
      version,
      timestamp: new Date().getTime()
    }))
  }).catch((err) => client.publish(getTopics(serialNumber).deleteRejected, JSON.stringify(errObj(err.toString()))))
}
