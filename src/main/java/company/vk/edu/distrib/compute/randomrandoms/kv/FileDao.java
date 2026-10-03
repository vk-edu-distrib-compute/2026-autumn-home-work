package company.vk.edu.distrib.compute.randomrandoms.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.NoSuchElementException;
import java.util.Optional;

public class FileDao implements Dao<byte[]> {
    private final OutputStream stream;
    private final RamDao<byte[]> ramDao;

    private static final String DELETE_END = "*";
    private static final String UPSERT_SEP = ":";
    private static final int UPSERT_WORDS = 2;
    private static final String WORD_END = "%";
    private static final String UPSERT_KEY_END = WORD_END + UPSERT_SEP;
    private static final String DELETE_KEY_END = WORD_END + DELETE_END;

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        return ramDao.get(key);
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        stream.write(Command.upsert(key, value).dump());
        ramDao.upsert(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        stream.write(Command.delete(key).dump());
        ramDao.delete(key);
    }

    @Override
    public void close() throws IOException {
        stream.close();
        ramDao.close();
    }

    private record Command(String key, Optional<byte[]> value) {
        public static Command upsert(String key, byte[] value) {
            return new Command(key, Optional.of(value));
        }

        public static Command delete(String key) {
            return new Command(key, Optional.empty());
        }

        public byte[] dump() {
            var sb = new StringBuilder();
            sb
                    .append(new String(Base64.getEncoder().encode(key.getBytes(StandardCharsets.UTF_8))));
            try {
                var val = value.orElseThrow();
                sb
                        .append(UPSERT_KEY_END)
                        .append(new String(Base64.getEncoder().encode(val), StandardCharsets.UTF_8))
                        .append(WORD_END);
            } catch (NoSuchElementException e) {
                sb.append(DELETE_KEY_END);
            }
            sb.append('\n');
            return sb.toString().getBytes(StandardCharsets.UTF_8);
        }

        public static Command fromDump(String cmd) {
            var split = cmd.split(UPSERT_SEP);
            if (split.length == UPSERT_WORDS) {
                String key = new String(
                        Base64
                                .getDecoder()
                                .decode(split[0].substring(0, split[0].length() - 1).getBytes(StandardCharsets.UTF_8)),
                        StandardCharsets.UTF_8
                );
                byte[] value = Base64.getDecoder().decode(split[1].substring(0, split[1].length() - 1));
                return new Command(key, Optional.of(value));
            }
            String key = cmd.substring(0, cmd.length() - 1);
            if (!DELETE_END.equals(cmd.substring(cmd.length() - 1))) {
                throw new IllegalStateException("incorrect dump");
            }
            return new Command(key, Optional.empty());
        }
    }

    private void run(RamDao<byte[]> dao, Command cmd) {
        try {
            var value = cmd.value.orElseThrow();
            dao.upsert(cmd.key, value);
        } catch (NoSuchElementException e) {
            dao.delete(cmd.key);
        }
    }

    public FileDao(Path fname) throws IOException {
        ramDao = new RamDao<>();
        Files.newOutputStream(fname, StandardOpenOption.CREATE, StandardOpenOption.APPEND).close();
        try (var reader = Files.newBufferedReader(fname)) {
            reader.lines().forEach(line -> run(ramDao, Command.fromDump(line)));
        }
        stream = Files.newOutputStream(fname, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
}
