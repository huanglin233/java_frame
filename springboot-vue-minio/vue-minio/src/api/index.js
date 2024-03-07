
const env = import.meta.env.MODE;
const envConfig = {
    development: {
        baseApi: 'http://127.0.0.1:9001/'
    },
    production: {
        baseApi: 'http://127.0.0.1:9001/'
    }
}

export default {
    env,
    ...envConfig[env]
}

