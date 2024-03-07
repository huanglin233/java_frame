package com.hl.springbootMinio.controller;

import com.hl.springbootMinio.common.Ret;
import com.hl.springbootMinio.service.MinioServiceImpl;
import com.hl.springbootMinio.vo.FileFragmentVo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

/**
 * @author huanglin
 * @date 2023/09/17 17:46
 */
@RestController
@CrossOrigin
@RequestMapping("/upload")
@Slf4j
public class UploadController {

    @Autowired
    MinioServiceImpl service;

    /**
     * 上传分片文件 -- 通过服务器上传
     * @param file        分片文件
     * @param md5         文件md5
     * @param curIndex    当前分片索引
     * @param totalPieces 分片总数
     * @return
     */
    @PostMapping(value = "/fragment", produces = MediaType.APPLICATION_JSON_VALUE)
    public Ret<FileFragmentVo> uploadFile(@RequestParam("file")MultipartFile file, @RequestParam("md5")String md5,
                                          @RequestParam("curIndex")Integer curIndex, @RequestParam("totalPieces")Integer totalPieces) {
        try {
            return Ret.success(service.uploadFileFragment(file, curIndex, totalPieces, md5), "分片上传成功");
        } catch (Exception e) {
            e.printStackTrace();
            log.error("分片文件上传失败");

            return Ret.fail("文件上传失败");
        }
    }

    /**
     * 合并通过服务器上传的分片文件
     * @param md5
     * @param name
     * @param totalPieces
     * @return
     */
    @GetMapping("/merge")
    public Ret<Integer> mergeFile(@RequestParam("md5")String md5, @RequestParam("name")String name, @RequestParam("totalPieces")Integer totalPieces) {
        try {
            Integer i = service.mergeFileFragment(name, totalPieces, md5);

            if(i == 1) {
                return Ret.success(i, "合并文件完成");
            } else {
                return Ret.success(i, "合并文件失败,分片文件上传不完整");
            }

        } catch (Exception e) {
            e.printStackTrace();
            log.error("合并文件失败");

            return Ret.fail("合并文件失败");
        }
    }
}
