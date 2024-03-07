package com.hl.springbootMinio.service;

import cn.hutool.core.convert.Convert;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Sets;
import com.hl.springbootMinio.utils.MinioUtils;
import com.hl.springbootMinio.vo.FileFragmentVo;
import io.minio.ComposeSource;
import io.minio.ObjectWriteResponse;
import io.minio.Result;
import io.minio.errors.*;
import io.minio.messages.DeleteError;
import io.minio.messages.Item;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author huanglin
 * @date 2023/09/03 20:11
 */
@Slf4j
@Service
public class MinioServiceImpl {

    /**
     * 最终文件桶
     */
    private static final String BUCKET = "hl233";
    /**
     * 分片存储的临时桶
     */
    private static final String TEMP_BUCKET = "hl233temp";

    @Autowired
    MinioUtils minioUtils;

    /**
     * 上传分片文件 -- 应用服务器中转上传
     * @param file        分片文件
     * @param currIndex   当前分片索引
     * @param totalPieces 分片总数
     * @param md5         分片得md5
     * @return 分片上传结果
     * @throws Exception
     */
    public FileFragmentVo uploadFileFragment(MultipartFile file, Integer currIndex,
                                              Integer totalPieces, String md5) throws Exception {
        //先判断桶是否存在
        if (!minioUtils.bucketExists(TEMP_BUCKET)) {
            minioUtils.createBucket(TEMP_BUCKET);
        }

        // 得到已经上传的文件索引
        Set<Integer> remainIndex = getNotUpIndex(md5, totalPieces);
        boolean      fileExists  = !remainIndex.contains(currIndex);

        FileFragmentVo fileFragmentVo = new FileFragmentVo();
        fileFragmentVo.setNotUploadIndex(remainIndex);
        fileFragmentVo.setCurUploadIndex(currIndex);
        if(fileExists) {
            fileFragmentVo.setState(2);
        } else {
            // 上传文件
            minioUtils.uploadFileStream(TEMP_BUCKET, getFileTempPath(md5, currIndex, totalPieces), file.getInputStream());
            // 如果上传成功去除掉已经上传
            remainIndex.remove(currIndex);
            fileFragmentVo.setNotUploadIndex(getNotUpIndex(md5, totalPieces));
            fileFragmentVo.setState(1);
        }

        return fileFragmentVo;
    }

    /**
     * 合并上传得分片临死文件--通过服务上传的方式的合并
     * @param targetName  合并后的文件名称(含全路径)
     * @param totalPieces 切片总数
     * @param md5         文件md5
     * @return 合并状态 1合并完成/-1合并失败/-2未合并,分片未上传完成
     */
    public Integer mergeFileFragment(String targetName, Integer totalPieces, String md5) throws Exception {
        // 1.给上传得文件key加上文件唯一md5路径--可以区别同文件名不同文件得覆盖
        targetName = md5 + "/" + targetName;
        // 2.首先检查分片文件是否上传完整
        Iterable<Result<Item>> tempFiles = minioUtils.getFilesByPrefix(TEMP_BUCKET, md5.concat("/"), false);
        Set<String> savedIndex = Sets.newHashSet();
        for(Result<Item> item : tempFiles) {
            savedIndex.add(item.get().objectName());
        }
        if(savedIndex.size() == totalPieces) {
            // 文件路径转文件合并对象
            List<ComposeSource> sourcesObjectList = savedIndex.stream()
                    .map(filePtah -> ComposeSource.builder()
                            .bucket(TEMP_BUCKET)
                            .object(filePtah).build()).collect(Collectors.toList());

            minioUtils.mergeFile(BUCKET, targetName, sourcesObjectList);
            // 合并完临时文件后删除临时分片文件
            List<String> tempPaths = Stream.iterate(1, i -> ++i)
                    .limit(totalPieces)
                    .map(i -> getFileTempPath(md5, i, totalPieces))
                    .collect(Collectors.toList());
            Iterable<Result<DeleteError>> deleteResults = minioUtils.removeFiles(TEMP_BUCKET, tempPaths);
            for(Result<DeleteError> error : deleteResults) {
                DeleteError deleteError = error.get();
                log.error("分片{}删除失败！错误信息: {}", deleteError.objectName(), deleteError.message());
            }

            return 1;
        } else {
            return -1;
        }
    }


    /**
     * 根据文件md5获取未上传得切片
     * @param md5   文件切片
     * @param total 切片总数
     * @return
     * @throws Exception
     */
    private Set<Integer> getNotUpIndex(String md5, Integer total) throws Exception {
        Iterable<Result<Item>> files      = minioUtils.getFilesByPrefix(TEMP_BUCKET, md5.concat("/"), false);
        Set<Integer>           savedIndex = Sets.newHashSet();
        for (Result<Item> item : files) {
            Integer idx = Integer.valueOf(Convert.toStr(StrUtil.split(item.get().objectName(), "/").get(1), ""));
            savedIndex.add(idx);
        }

        // 得到未上传的文件索引
        Set<Integer> remainIndex = Sets.newTreeSet();
        for(int i = 1; i <= total; i++) {
            if(!savedIndex.contains(i)) {
                remainIndex.add(i);
            }
        }

        return remainIndex;
    }

    /**
     * 通过文件得md5,以及分片文件的索引算出临时分片文件得存储路径
     * @param md5         分片文件得md5
     * @param currIndex   当前分片索引
     * @param totalPieces 分片总数
     * @return 切片临时文件存储路径
     */
    private String getFileTempPath(String md5, Integer currIndex, Integer totalPieces) {
        int           size      = 10;
        StringBuilder tempPath  = new StringBuilder(md5);
        tempPath.append("/");
        if(totalPieces <= size && currIndex < size) {
            tempPath.append(0);
        }
        tempPath.append(currIndex);

        return tempPath.toString();
    }
}
