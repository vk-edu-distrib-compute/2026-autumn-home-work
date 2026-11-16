package company.vk.edu.distrib.compute.allpandasarecute.kv;

import java.io.IOException;
import java.nio.file.Path;

import company.vk.edu.distrib.compute.allpandasarecute.common.AbstractFileDao;

public class FileBytesDao extends AbstractFileDao<byte[]> {
    public FileBytesDao(Path directory) throws IOException {
        super(directory);
    }

    @Override
    public void close() {
        //
    }

    @Override
    protected byte[] serialize(byte[] value) {
        return value;
    }

    @Override
    protected byte[] deserialize(byte[] data) {
        return data;
    }
}
