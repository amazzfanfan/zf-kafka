// Real HTTP + PostgreSQL pipeline benchmark. Creates isolated test configuration,
// then disables it. Never points at an external webhook.
import {createServer} from 'node:http'
import {readFileSync} from 'node:fs'
import {performance} from 'node:perf_hooks'
import {execFile} from 'node:child_process'
const token=process.env.RELAY_ADMIN_TOKEN||readFileSync('.local/dev.env','utf8').match(/RELAY_ADMIN_TOKEN=(\w+)/)[1]
const prefix=process.env.APP_PREFIX||'bridge',base=process.env.RELAY_LOAD_URL||'http://127.0.0.1:18080'
const count=Number(process.env.RELAY_LOAD_COUNT||1000),parallel=Number(process.env.RELAY_LOAD_PARALLEL||16)
if(!Number.isInteger(count)||count<1||count>100000||parallel<1||parallel>128)throw new Error('Invalid count/concurrency')
const latencies=[],ids=new Set();let received=0
let peakRSS=0
const monitor=process.env.RELAY_LOAD_PID?setInterval(()=>execFile('ps',['-p',process.env.RELAY_LOAD_PID,'-o','rss='],(e,out)=>{if(!e)peakRSS=Math.max(peakRSS,Number(out.trim())||0)}),100):null
const sink=createServer(async(req,res)=>{let body='';for await(const b of req)body+=b;const v=JSON.parse(body);latencies.push(Date.now()-v.sent_at);received++;ids.add(req.headers['idempotency-key']);res.writeHead(204);res.end()})
await new Promise(resolve=>sink.listen(0,'127.0.0.1',resolve))
const address=sink.address(),configs=[]
async function api(path,method='GET',data){const r=await fetch(`${base}/api/${prefix}/v1${path}`,{method,signal:AbortSignal.timeout(15000),headers:{Authorization:`Bearer ${token}`,'Content-Type':'application/json'},body:data===undefined?undefined:JSON.stringify(data)});const v=await r.json();if(!r.ok)throw new Error(`${path}: ${r.status} ${v.error}`);return v.data}
let failed=false
try{
 const source=await api('/sources','POST',{name:'基准测试来源',type:'manual',enabled:true});configs.push(['sources',source])
 const endpoint=await api('/endpoints','POST',{name:'本地基准目标',enabled:true,url:`http://127.0.0.1:${address.port}/`,method:'POST',body_mode:'raw',headers:{'Content-Type':'application/json'},auth:{type:'none',credential_ref:''},allow_private:true,concurrency:64,rps:10000,max_attempts:3,idempotency_header:'Idempotency-Key'});configs.push(['endpoints',endpoint])
 const route=await api('/routes','POST',{name:'基准转发',enabled:true,source_id:source.id,endpoint_ids:[endpoint.id]});configs.push(['routes',route])
 const start=performance.now();let next=0
 await Promise.all(Array.from({length:parallel},async()=>{while(next<count){const n=next++;await api(`/sources/${source.id}/messages`,'POST',{body:JSON.stringify({n,sent_at:Date.now(),payload:'x'.repeat(900)}),dedup_key:`load-${n}`})}}))
 const admitted=performance.now();const deadline=performance.now()+90000
 while(received<count&&performance.now()<deadline)await new Promise(resolve=>setTimeout(resolve,50))
 if(received!==count||ids.size!==count)throw new Error(`Expected ${count} deliveries; received=${received}, unique=${ids.size}`)
 latencies.sort((a,b)=>a-b)
 console.log(JSON.stringify({messages:count,payload_bytes:'approximately 950 bytes',producer_concurrency:parallel,admission_seconds:+((admitted-start)/1000).toFixed(3),end_to_end_seconds:+((performance.now()-start)/1000).toFixed(3),messages_per_second:+(count/((performance.now()-start)/1000)).toFixed(1),p50_ms:latencies[Math.floor(latencies.length*.5)],p95_ms:latencies[Math.floor(latencies.length*.95)],p99_ms:latencies[Math.floor(latencies.length*.99)],duplicate_deliveries:received-ids.size,app_peak_rss_mib:peakRSS?+(peakRSS/1024).toFixed(1):null},null,2))
}catch(e){failed=true;console.error(e.message)}finally{if(monitor)clearInterval(monitor);for(const [kind,c]of configs.reverse())try{await api(`/${kind}/${c.id}`,'PUT',{...c,enabled:false})}catch{};sink.closeAllConnections();await new Promise(resolve=>sink.close(resolve))}
if(failed)process.exitCode=1
