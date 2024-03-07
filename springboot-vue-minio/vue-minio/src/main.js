import { createApp } from 'vue'
import './style.css'
import ElementPlus from 'element-plus';
import 'element-plus/dist/index.css';
import App from './App.vue'

const app = createApp(App);

app.use(ElementPlus);
app.mount("#app");

// 配置axios
import axios from 'axios';
app.config.globalProperties.$http = axios;