import {afterEach,describe,it,expect,vi} from 'vitest'
import {request} from './api'
afterEach(()=>vi.unstubAllGlobals())
describe('management API contract',()=>{
 it('uses the deployment prefix and preserves raw message text',async()=>{
  vi.stubGlobal('location',{pathname:'/bridge/debug'})
  const fetcher=vi.fn().mockResolvedValue(new Response(JSON.stringify({data:{event_id:'event-1'}}),{status:200}))
  vi.stubGlobal('fetch',fetcher)
  const body='{ "id":9007199254740993, "amount":"12.30" }'
  const result=await request('/sources/source-1/messages','POST',{body})
  expect(fetcher.mock.calls[0][0]).toBe('/api/bridge/v1/sources/source-1/messages')
  expect(JSON.parse(fetcher.mock.calls[0][1].body).body).toBe(body)
  expect(result).toEqual({event_id:'event-1'})
 })
 it('surfaces authorization failures instead of treating them as success',async()=>{
  vi.stubGlobal('location',{pathname:'/bridge/sources'})
  vi.stubGlobal('fetch',vi.fn().mockResolvedValue(new Response(JSON.stringify({error:'same-origin request required'}),{status:403})))
  await expect(request('/sources','POST',{})).rejects.toThrow('same-origin request required')
 })
})
