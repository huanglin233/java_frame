import request from './request.js'

export default {
    uploadFragment(data){
        return request("/upload/fragment", {
            method: 'post',
            data: data,
            headers: {
                'Content-Type': 'multipart/form-data'
            }
        })
    },
    mergeFile(data) {
        return request("/upload/merge", {
            method: 'get',
            data: data
        })
    }
}