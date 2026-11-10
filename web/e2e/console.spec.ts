import {test,expect} from '@playwright/test'
import {readFileSync} from 'node:fs'
const token=process.env.RELAY_BROWSER_TOKEN||readFileSync('../.local/dev.env','utf8').match(/RELAY_ADMIN_TOKEN=(\w+)/)![1]
test('prefix, real configuration, preview and delivery workflow',async({page,request})=>{
 const prefix=process.env.APP_PREFIX||'bridge'
 const api=`/api/${prefix}/v1`
 const headers={Authorization:`Bearer ${token}`}
 const errors:string[]=[]
 page.on('pageerror',e=>errors.push(e.message))
 const srcResp=await request.post(api+'/sources',{headers,data:{name:'端到端测试来源',type:'manual',enabled:true}})
 expect(srcResp.ok()).toBeTruthy();const source=(await srcResp.json()).data
 const epResp=await request.post(api+'/endpoints',{headers,data:{name:'请求预览测试',enabled:false,url:'https://example.com/hooks/order',method:'POST',body_mode:'template',body:'{"orderNo":{{json .JSON.order_id}}}',headers:{'Content-Type':'application/json'},auth:{type:'none',credential_ref:''}}})
 expect(epResp.ok()).toBeTruthy();const endpoint=(await epResp.json()).data
 const routeResp=await request.post(api+'/routes',{headers,data:{name:'端到端规则',enabled:true,source_id:source.id,endpoint_ids:[endpoint.id]}})
 expect(routeResp.ok()).toBeTruthy()
 await page.goto(`/${prefix}/`)
 await expect(page.getByText('正在连接 Relay…',{exact:true})).toBeHidden()
 if(await page.getByPlaceholder('输入 RELAY_ADMIN_TOKEN').isVisible()){await page.getByPlaceholder('输入 RELAY_ADMIN_TOKEN').fill(token);await page.getByRole('button',{name:'进入控制台'}).click()}
 await expect(page.getByRole('heading',{name:'运行概览',exact:true})).toBeVisible()
 await page.screenshot({path:'../docs/console-overview.png',fullPage:true})
 await page.getByRole('link',{name:'请求调试',exact:true}).click()
 await page.locator('select').first().selectOption(endpoint.id)
 await page.getByRole('button',{name:'预览请求',exact:true}).click()
 await expect(page.locator('pre')).toContainText('orderNo')
 await page.locator('select').last().selectOption(source.id)
 await page.getByRole('button',{name:'接收样例并触发真实转发'}).click()
 await expect(page.getByRole('status')).toContainText('消息已持久化接收')
 await page.getByRole('link',{name:/投递记录/}).click()
 await expect(page.getByText('PENDING',{exact:true}).first()).toBeVisible()
 await page.getByRole('button',{name:'详情',exact:true}).first().click()
 await expect(page.getByRole('heading',{name:'投递尝试记录'})).toBeVisible()
 await page.getByRole('button',{name:'关闭',exact:true}).click()
 await page.reload()
 await expect(page.getByRole('heading',{name:'投递记录',exact:true})).toBeVisible()
 await page.setViewportSize({width:390,height:844});await page.screenshot({path:'../docs/console-mobile.png',fullPage:true})
 expect(errors).toEqual([])
 const missing=await request.get(api+'/not-found',{headers});expect(missing.status()).toBe(404);expect(missing.headers()['content-type']).toContain('application/json')
})
