import {defineConfig} from '@playwright/test'
export default defineConfig({testDir:'./e2e',timeout:60000,use:{baseURL:process.env.RELAY_BROWSER_URL||'http://127.0.0.1:18080',viewport:{width:1440,height:1000}},workers:1,reporter:'list'})
