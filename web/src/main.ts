import {createApp} from 'vue'
import {createRouter,createWebHistory} from 'vue-router'
import App from './App.vue'
import './style.css'
const prefix=location.pathname.split('/').filter(Boolean)[0]||'relay'
const response=await fetch(`/${prefix}/runtime-config.json`)
if(!response.ok)throw new Error('无法加载运行时配置，请通过后端地址访问控制台')
const {data:config}=await response.json()
export const apiBase:string=config.api_base
const router=createRouter({history:createWebHistory(`/${config.prefix}/`),routes:[{path:'/:page?',component:{template:'<span />'}}]})
createApp(App).use(router).mount('#app')
