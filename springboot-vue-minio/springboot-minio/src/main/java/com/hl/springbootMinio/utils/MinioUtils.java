package com.hl.springbootMinio.utils;

import cn.hutool.core.util.StrUtil;
import io.minio.*;
import io.minio.http.Method;
import io.minio.messages.DeleteError;
import io.minio.messages.DeleteObject;
import io.minio.messages.Item;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author huanglin
 * @date 2023/09/03 20:53
 */
public class MinioUtils {

    private final MinioClient client;

    public MinioUtils(MinioClient client) {
        this.client = client;
    }

    /**
     * 判断bucket是否存在
     *
     * @param bucketName 桶名称
     * @return true存在/false不存在
     * @throws Exception
     */
    public boolean bucketExists(String bucketName) throws Exception {
        return client.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
    }

    /**
     * 如果桶不存在就创建一个桶
     *
     * @param bucketName 桶名称
     * @throws Exception
     */
    public void createBucket(String bucketName) throws Exception {
        if (!bucketExists(bucketName)) {
            client.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
        }
    }

    /**
     * 通过流上传文件
     *
     * @param bucketName  桶名称
     * @param fileName    文件名称
     * @param inputStream 文件数据流
     * @return
     * @throws Exception
     */
    public ObjectWriteResponse uploadFileStream(String bucketName, String fileName, InputStream inputStream) throws Exception {
        return client.putObject(
                PutObjectArgs.builder()
                        .bucket(bucketName)
                        .object(fileName)
                        .stream(inputStream, inputStream.available(), -1)
                        .build());
    }

    /**
     * 批量删除文件
     *
     * @param bucketName 桶名
     * @param filePaths  需要删除得文件列表
     * @return
     */
    public Iterable<Result<DeleteError>> removeFiles(String bucketName, List<String> filePaths) {
        List<DeleteObject> deleteList = filePaths.stream()
                .map(filePath -> new DeleteObject(filePath))
                .collect(Collectors.toList());

        return client.removeObjects(RemoveObjectsArgs
                .builder()
                .bucket(bucketName)
                .objects(deleteList)
                .build());
    }

    /**
     * 根据文件前缀获取
     *
     * @param bucketName  桶名称
     * @param prefix      文件前缀
     * @param isRecursive 是否递归
     * @return
     */
    public Iterable<Result<Item>> getFilesByPrefix(String bucketName, String prefix, boolean isRecursive) {
        return client.listObjects(
                ListObjectsArgs.builder()
                        .bucket(bucketName)
                        .prefix(prefix)
                        .recursive(isRecursive)
                        .build()
        );
    }

    /**
     * 合并文件
     *
     * @param bucketName       桶名
     * @param targetName       文件路径名称
     * @param sourceObjectList 分片文件
     * @return
     * @throws Exception
     */
    public ObjectWriteResponse mergeFile(String bucketName, String targetName, List<ComposeSource> sourceObjectList) throws Exception {
        return client.composeObject(ComposeObjectArgs.builder()
                .bucket(bucketName)
                .object(targetName)
                .sources(sourceObjectList)
                .build());
    }

    /**
     * 获取minio直传单文件签名-1小时有效
     *
     * @param bucketName  桶名
     * @param objectName  上传文件路径
     * @param contentType 上传文件携带content-type头
     * @return
     */
    public String getUploadUrl(String bucketName, String objectName, String contentType) throws Exception {
        if (StrUtil.isEmpty(contentType)) {
            // 默认设置上传文件携带的content-type头
            contentType = "application/octet-stream";
        }
        Map<String, String> headers = new HashMap<>(1);
        headers.put("Content-Type", contentType);
        GetPresignedObjectUrlArgs build = GetPresignedObjectUrlArgs
                .builder()
                .method(Method.PUT)
                .bucket(bucketName)
                .object(objectName)
                .expiry(1, TimeUnit.HOURS)
                .extraHeaders(headers)
                .build();

        return client.getPresignedObjectUrl(build);
    }
}
