<script setup>
    import {ref, onMounted} from 'vue';
    import { ElMessage, ElMessageBox } from 'element-plus';

    import http from '../api/api.js';

    // ui组件
    let files = ref([]);
    // 需要上传的文件列表
    let uploadFiles = [];
    // 多文件循环计算hash是文件索引
    let fileIndex = 0;
    // 最小切片
    const chunkSize = 5 * 1024 * 1024;
    let worker;


    /**
     * 删除文件
     * @param file
     * @param uploadFiles
     */
    const handleRemove = (file, uploadFiles) => {
        console.log(file, uploadFiles);
    }

    /**
     * 上传超出限制
     * @param files
     * @param uploadFiles
     */
    const handleExceed = (files, uploadFiles) => {
        ElMessage.warning(
            `The limit is 1, you selected ${files.length} files this time, add up to ${
                files.length + uploadFiles.length
            } totally`
        );
    }

    /**
     * 删除文件前
     * @param uploadFile
     * @param uploadFiles
     * @returns {Promise<boolean>}
     */
    const beforeRemove = (uploadFile, uploadFiles) => {
        return ElMessageBox.confirm(`Cancel the transfer of ${uploadFile.name} ?`)
            .then(
                () => true,
                () => false
            )
    }

    /**
     * 上传文件
     */
    const uploadFile = async () => {
        if (!files) {
            return;
        }
        for (let i = 0; i < files.value.length; i++) {
            const file          = files.value[i].raw;
            const fileChunkList = creatFileChunk(file);
            fileIndex = i;
            const md5Hash = await createFileHash(fileChunkList);
            console.log("》》》》》》开始上传文件");
            const total = fileChunkList.length;
            let successNum = 0;
            for(let i = 0; i < total; i++) {
                let params = new FormData();
                params.append("file", fileChunkList[i].file);
                params.append("md5", md5Hash);
                params.append("curIndex", i + 1);
                params.append("totalPieces", total);
                http.uploadFragment(params).then(res => {
                    if(res.state == 1 || res.state == 2) {
                        successNum += 1;
                        if(successNum == total && res.notUploadIndex.length < 1) {
                            // 文件上传完毕,合并文件
                            const params = {
                                md5: md5Hash,
                                name: file.name,
                                totalPieces: total
                            }
                            console.log(params);
                            http.mergeFile(params).then(res => {
                                console.log(res);
                            })
                        }
                    }
                })
            }
        }
    }

    /**
     * 对文件进行切片
     * @param file 文件
     * @param size 每个切片最小值
     * @returns {*[]}
     */
    const creatFileChunk = (file, size = chunkSize) => {
        const fileChunkList = [];
        let count = 0;
        if(count < file.size) {
            const chunkNum = parseInt(file.size / size);
            if(chunkNum > 1) {
                for(let i = 1; i <= chunkNum; i++) {
                    if(i === chunkNum) {
                        fileChunkList.push({file: file.slice(count, file.size)});
                    } else {
                        fileChunkList.push({file: file.slice(count, count + size)})
                    }

                    count += size;
                }
            } else {
                fileChunkList.push({file: file.slice(count, file.size)})
            }
        }

        return fileChunkList;
    }

    // 生成文件hash
    const createFileHash = fileChunkList => {
        return new Promise(resolve => {
            worker = new Worker('../utils/md5Hash.js')
            worker.postMessage({fileChunkList});
            worker.onmessage = (e) => {
                const {percentage, hash} = e.data;
                if(uploadFiles[fileIndex]) {
                    uploadFiles[fileIndex].hashProgress = Number(percentage.toFixed(0));
                }

                if(hash) {
                    resolve(hash);
                }
            }
        })
    }

    onMounted(() => {
    })

    let arr = ref([]);
    setTimeout(() => {
      arr.value.push(233);
      arr.value.push(332)
    }, 1000);
    setTimeout(() => {
      arr.value.push(233);
      arr.value.push(332)
    }, 10000);
</script>

<template>
    <div>
        <div style="width: 100%; text-align: center">
            文件上传233
            <el-upload v-model:file-list="files"
                       :on-remove="handleRemove"
                       :before-remove="beforeRemove"
                       :limit="1"
                       :on-exceed="handleExceed"
                       :auto-upload="false"
            >
                <el-button type="primary">选择文件</el-button>
                <template #tip>
                    <div class="el-upload__tip">
                        files with a size greater than 5m
                    </div>
                </template>
            </el-upload>
            <el-button type="success" @click="uploadFile">上传</el-button>
        </div>
      <div v-for="item in arr">
        {{item}}
      </div>
    </div>
</template>

<style scoped>

</style>