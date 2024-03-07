package com.hl.springbootMinio.vo;

import lombok.Data;

import java.util.Set;

/**
 * 分片上传结果实体类
 * @author huanglin
 * @date 2023/09/03 20:13
 */

@Data
public class FileFragmentVo {

    /**
     * 状态 -1分片上传失败;1上传成功;2已经上传成功
     */
    Integer state;

    /**
     * 未上传的分片索引
     */
    Set<Integer> notUploadIndex;

    /**
     * 当前上传的分片索引
     */
    Integer curUploadIndex;
}
