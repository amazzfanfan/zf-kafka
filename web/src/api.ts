export async function request<T=any>(path:string,method='GET',body?:unknown):Promise<T>{
 const prefix=location.pathname.split('/').filter(Boolean)[0]||'relay'
 const r=await fetch(`/api/${prefix}/v1${path}`,{method,headers:{'Content-Type':'application/json'},body:body===undefined?undefined:JSON.stringify(body)})
 const v=await r.json()
 if(!r.ok)throw new Error(v.error||`HTTP ${r.status}`)
 return v.data
}
export function pretty(v:unknown){return JSON.stringify(v,null,2)}
export function statusTone(s:string){return ['SUCCEEDED','RUNNING'].includes(s)?'green':['DEAD','ERROR','UNKNOWN'].includes(s)?'red':['PENDING','RETRY_WAIT','SENDING','CONNECTING'].includes(s)?'amber':'gray'}
