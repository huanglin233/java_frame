import axios from 'axios';
import config from "./index.js";
import {ElMessage} from 'element-plus';

// 创建axios
const service = axios.create({
    baseURL: config.baseApi
});

// 请求前
service.interceptors.request.use((req) => {
    // 可自定义header
    // req.headers["token"] = "token";
    return req;
});

// 请求之后
service.interceptors.response.use((res) => {
    const {code, data, msg} = res.data;
    if(code === 200) {
        return data;
    } else {
        ElMessage.error(msg);
        return Promise.reject(msg);
    }
}, (error) => {
    if(error && error.response) {
        switch (error.response.status) {
            case 403:
                error.message = '拒绝访问';
                break;
            case 502:
                error.message = '服务端出错';
                break;
            default:
                error.message = `连接错误${error.response.status}`;
        }
    } else {
        error.message = '服务器响应超时，请刷新当前页面';
    }

    ElMessage.error(error.message);
    return Promise.resolve(error.response);
});

// 封装请求函数
function request(url, options = {}) {
    const method = options.method || 'get';
    const params = options.params || options.data || {};
    const headers = options.headers || options.headers || {};

    if(method === 'get' || method === 'GET') {
        return new Promise((resolve, reject) => {
            service.get(url, {
                params: params
            }).then(res => {
                if(res) {
                    resolve(res);
                }
            }).catch(error => {
                reject(error);
            });
        });
    } else {
        return new Promise((resolve, reject) => {
            service.post(url, params, {...headers}).then(res => {
                if(res) {
                    resolve(res);
                }
            }).catch(error => {
                reject(error);
            })
        })
    }

    return service(options);
}

export default request;