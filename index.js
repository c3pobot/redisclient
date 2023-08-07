'use strict'
const log = require('./logger')
let logLevel = process.env.LOG_LEVEL || log.Level.INFO;
log.setLevel(logLevel);

const Cmds = {}
const NodeRedis = require('redis')

let redisReady = false, notify = true
const redisOpts = {
  socket: {
    host: process.env.REDIS_SERVER,
    port: process.env.REDIS_PORT
  }
}
if(process.env.REDIS_PASS) redisOpts.socket.passwd = process.env.REDIS_PASS
const redis = NodeRedis.createClient(redisOpts)
redis.on('error', err=>{
  log.error(`redis client error ${err}`)
})
redis.on('connect', ()=>{
  log.debug(`redis client is connected`)
})
redis.on('reconnecting', ()=>{
  log.debug(`redis client is reconnecting`)
})
redis.on('ready', ()=>{
  if(notify){
    notify = false
    log.info(`redis client is ready`)
  }else{
    log.debug(`redis client is ready`)
  }
})
const StartClient = async()=>{
  try{
    await redis.init()
    let status = await redis.ping()
    if(status === 'PONG'){
      redisReady = true
      return
    }
    setTimeout(StartClient, 5000)
  }catch(e){
    log.error(e)
    setTimeout(StartClient, 5000)
  }
}
StartClient()
Cmds.bpull = async(key)=>{
  try{
    let obj = await redis.BRPOP(key, 5.00)
    if(obj && obj[1]) return JSON.parse(obj[1])
  }catch(e){
    throw(e)
  }
}
Cmds.del = async(key)=>{
  try{
    return await redis.del(key)
  }catch(e){
    throw(e)
  }
}
Cmds.get = async(key)=>{
  try{
    let obj = await redis.get(key)
    if(obj) return JSON.parse(obj)
  }catch(e){
    throw(e)
  }
}
Cmds.getList = async(key)=>{
  try{
    return await redis.LRANGE(key, 0, -1)
  }catch(e){
    throw(e)
  }
}

Cmds.hget = async(key)=>{
  try{
    return await redis.HGETALL(key)
  }catch(e){
    throw(e)
  }
}
Cmds.init = async()=>{
  try{
    return await redis.connect()
  }catch(e){
    throw(e)
  }
}
Cmds.keys = async(str)=>{
  try{
    return await redis.KEYS(str)
  }catch(e){
    throw(e)
  }
}
Cmds.ping = async()=>{
  try{
    return await redis.ping()
  }catch(e){
    throw(e)
  }
}
Cmds.priority = async(key, obj = {})=>{
  try{
    if(!key) return
    return await redis.RPUSH(key, JSON.stringify(obj))
  }catch(e){
    throw(e)
  }
}
Cmds.pull = async(key)=>{
  try{
    let obj = await redis.RPOP(key)
    if(obj) return JSON.parse(obj)
  }catch(e){
    throw(e)
  }
}
Cmds.push = async(key, obj = {})=>{
  try{
    if(!key) return
    return await redis.LPUSH(key, JSON.stringify(obj))
  }catch(e){
    throw(e)
  }
}
Cmds.rem = async(key, obj)=>{
  try{
    return await redis.LREM(key, -1, JSON.stringify(obj))
  }catch(e){
    throw(e)
  }
}
Cmds.set = async(key, obj = {})=>{
  try{
    if(!key) return
    return await redis.set(key, JSON.stringify(obj))
  }catch(e){
    throw(e)
  }
}
Cmds.setTTL = async(key, obj = {}, ttl = 600)=>{
  try{
    if(!key) return
    return await redis.SET(key, JSON.stringify(obj), { EX: ttl })
  }catch(e){
    throw(e)
  }
}
Cmds.status = ()=>{
  return redisReady
}
Cmds.string = async(key, string)=>{
  try{
    if(!key || !string) return
    return await redis.set(key, string)
  }catch(e){
    throw(e)
  }
}
module.exports = Cmds
