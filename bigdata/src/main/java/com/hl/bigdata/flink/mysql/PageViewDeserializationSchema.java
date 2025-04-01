package com.hl.bigdata.flink.mysql;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

/**
 * @author huanglin
 * @date 2025/04/01 22:43
 */
public class PageViewDeserializationSchema implements DeserializationSchema {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Object deserialize(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, PageView.class);
    }

    @Override
    public boolean isEndOfStream(Object o) {
        return false;
    }

    @Override
    public TypeInformation getProducedType() {
        return TypeInformation.of(PageView.class);
    }
}
