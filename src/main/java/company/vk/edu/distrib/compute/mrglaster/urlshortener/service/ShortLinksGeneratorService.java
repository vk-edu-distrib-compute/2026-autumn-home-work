package company.vk.edu.distrib.compute.mrglaster.urlshortener.service;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

public class ShortLinksGeneratorService {

    private static final int ID_LENGTH = 10;
    private static final String ALLOWED_SYMBOLS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final char[] ALPHABET = ALLOWED_SYMBOLS.toCharArray();
    private static final SecureRandom RANDOM = new SecureRandom();

    private final String baseUrl;

    public ShortLinksGeneratorService(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public String generateLinkID(String longUrl) throws NoSuchAlgorithmException {
        StringBuilder sb = new StringBuilder(ID_LENGTH);
        for (int i = 0; i < ID_LENGTH; i++) {
            sb.append(ALPHABET[RANDOM.nextInt(ALPHABET.length)]);
        }
        return sb.toString();
    }

    public String generateShortURL(String shortId) {
        return baseUrl + "/" + shortId;
    }
}
