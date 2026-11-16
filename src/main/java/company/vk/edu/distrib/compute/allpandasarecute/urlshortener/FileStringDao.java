package company.vk.edu.distrib.compute.allpandasarecute.urlshortener;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import company.vk.edu.distrib.compute.allpandasarecute.common.AbstractFileDao;

public class FileStringDao extends AbstractFileDao<String> {
    public FileStringDao(Path directory) throws IOException {
        super(directory);
    }

    @Override
    public void close() {
        //
    }

    @Override
    protected byte[] serialize(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    protected String deserialize(byte[] data) {
        return new String(data, StandardCharsets.UTF_8);
    }
}
