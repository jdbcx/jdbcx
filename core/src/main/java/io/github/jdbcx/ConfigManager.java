/*
 * Copyright 2022-2025, Zhichun Wu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.jdbcx;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.AlgorithmParameterSpec;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;
import java.util.regex.Pattern;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.JwtParserBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import io.jsonwebtoken.security.SecureDigestAlgorithm;

/**
 * Manages application configurations using a two-tier structure.
 * 
 * Configurations are organized into categories and identified by unique IDs.
 * The category and ID structure is implementation-dependent:
 * 
 * <ul>
 * <li>For file system storage, the category is the folder name.
 * The ID is the file name for an individual configuration file.</li>
 *
 * <li>For database storage, the category could map to a table or column name.
 * The ID would be the primary key of a configuration row in the table.</li>
 * </ul>
 * 
 * This class provides methods to load and retrieve configurations using the
 * category/ID values. The underlying storage mechanism is abstracted.
 */
public abstract class ConfigManager {
    static final class JwtAlgorithm {
        final SecureDigestAlgorithm<? super SecretKey, ?> alg;
        final SecretKey key;

        JwtAlgorithm(SecureDigestAlgorithm<? super SecretKey, ?> alg, SecretKey key) {
            this.alg = alg;
            this.key = key;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = prime + alg.hashCode();
            result = prime * result + ((key == null) ? 0 : key.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            } else if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            JwtAlgorithm other = (JwtAlgorithm) obj;
            return alg.equals(other.alg) && Objects.equals(key, other.key);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ConfigManager.class);

    private static final SecureRandom random = new SecureRandom();

    private static final Option OPTION_CACHE_SIZE = Option.of("globpattern.cache.size", "Glob pattern cache size",
            "100");
    private static final Option OPTION_CACHE_EXPTIME = Option.of("globpattern.cache.exptime",
            "Glob pattern cache expiration time in second", "0");
    private static final Cache<String, Pattern> cache = Cache.create(
            Integer.parseInt(OPTION_CACHE_SIZE.getEffectiveDefaultValue(Option.PROPERTY_PREFIX)),
            Integer.parseInt(OPTION_CACHE_EXPTIME.getEffectiveDefaultValue(Option.PROPERTY_PREFIX)),
            ConfigManager::parseGlobPattern);

    private static final String ALGORITHM_AES = "AES";
    private static final String ALGORITHM_CHACHA20 = "ChaCha20";

    private static final Map<String, Properties> tenants = new ConcurrentHashMap<>();

    static final String ALGORITHM_AES_GCM_NOPADDING = ALGORITHM_AES + "/GCM/NoPadding";
    static final String ALGORITHM_CHACHA20_POLY1305 = ALGORITHM_CHACHA20 + "-Poly1305";

    static final Option OPTION_CONFIG_PROVIDER = Option.of("config.provider", "The class to manage configuration.",
            ConfigManager.class.getPackage().getName() + ".config.PropertyFile" + ConfigManager.class.getSimpleName());
    static final Option OPTION_KEY_FILE = Option.of("key.file", "Secret key file for configuration encryption.",
            Constants.CONF_DIR + "/secret.key");
    static final Option OPTION_SECRETS_FILE = Option.of("secrets.file", "Secrets JSON file.",
            Constants.CONF_DIR + "/secrets.json");
    static final Option OPTION_KEY_SIZE_BITS = Option
            .ofInt("encryption.key.bits", "Key size in bits", 256); // 32 bytes
    static final Option OPTION_IV_LENGTH_BYTES = Option
            .ofInt("encryption.iv.bytes", "Recommended IV length in bytes", 12); // 96 bits
    static final Option OPTION_TAG_LENGTH_BITS = Option
            .ofInt("encryption.tag.bits", "Authentication tag length in bits", 128); // 16 bytes
    static final Option OPTION_ALGORITHM = Option.of("encryption.algorithm", "Encryption algorithm.",
            ALGORITHM_AES_GCM_NOPADDING, ALGORITHM_AES_GCM_NOPADDING, ALGORITHM_CHACHA20_POLY1305);

    public static final String PROPERTY_ENCRYPTED_SUFFIX = ".encrypted";

    public static final Option OPTION_CACHE = Option
            .of(new String[] { "cache",
                    "Whether to load configuration into cache and use background thread to monitor changes" });
    public static final Option OPTION_ALIAS = Option.of(new String[] { "alias", "Comma separated aliases" });
    public static final Option OPTION_MANAGED = Option
            .of(new String[] { "manage", "Whether all the configuration are managed", Constants.FALSE_EXPR,
                    Constants.TRUE_EXPR });

    /**
     * Loads configuration from the given file. It's a no-op when {@code fileName}
     * is null or empty string.
     *
     * @param fileName file name
     * @param baseDir  optional base directory to search the file, null or empty
     *                 string is same as {@link Constants#CURRENT_DIR}
     * @param base     optional base configuration, which is parent of the returned
     *                 properties
     * @return non-null configuration
     */
    public static final Properties loadConfig(String fileName, String baseDir, Properties base) {
        fileName = Utils.normalizePath(fileName);

        Properties config = new Properties(base);
        if (Checker.isNullOrEmpty(fileName)) {
            log.debug("No config file specified");
        } else {
            Path path = Paths.get(fileName);
            if (!path.isAbsolute()) {
                path = Paths.get(Checker.isNullOrEmpty(baseDir) ? Constants.CURRENT_DIR : baseDir, fileName)
                        .normalize();
            }
            File file = path.toFile();
            if (file.exists() && file.canRead()) {
                try (Reader reader = new InputStreamReader(new FileInputStream(file), Constants.DEFAULT_CHARSET)) {
                    config.load(reader);
                    log.debug("Loaded config from file \"%s\".", fileName);
                } catch (IOException e) {
                    log.warn("Failed to load config from file \"%s\"", fileName, e);
                }
            } else {
                log.debug("Skip loading config as file \"%s\" is not accessible.", fileName);
            }
        }
        return config;
    }

    public static final ConfigManager newInstance(Properties props) {
        if (props == null) {
            props = new Properties();
        }
        return Utils.newInstance(ConfigManager.class, OPTION_CONFIG_PROVIDER.getJdbcxValue(props), props);
    }

    public static final Pattern parseGlobPattern(String str) {
        if (Checker.isNullOrEmpty(str)) {
            return null;
        }

        final int len = str.length();
        StringBuilder builder = new StringBuilder(len + 4);
        boolean inCharClass = false;

        for (int i = 0; i < len; i++) {
            char ch = str.charAt(i);
            switch (ch) {
                case '*':
                    if (inCharClass) {
                        builder.append(ch);
                    } else {
                        builder.append(".*");
                    }
                    break;
                case '?':
                    if (inCharClass) {
                        builder.append(ch);
                    } else {
                        builder.append('.');
                    }
                    break;
                case '[':
                    inCharClass = true;
                    builder.append(ch);
                    // Handle negated character class
                    if (i + 1 < len && str.charAt(i + 1) == '!') {
                        builder.append('^');
                        i++;
                    }
                    break;
                case ']':
                    inCharClass = false;
                    builder.append(ch);
                    break;
                case '\\':
                    if (i + 1 < len) {
                        builder.append('\\').append(str.charAt(++i));
                    } else {
                        builder.append('\\');
                    }
                    break;
                case '-':
                    if (!inCharClass) {
                        builder.append('\\').append(ch);
                    } else {
                        builder.append(ch);
                    }
                    break;
                // Escape regex special characters
                case '.':
                case '^':
                case '$':
                case '+':
                case '{':
                case '}':
                case '(':
                case ')':
                case '|': // NOSONAR
                    builder.append('\\').append(ch);
                    break;
                default:
                    builder.append(ch);
            }
        }
        return Pattern.compile(builder.toString());
    }

    static byte[] concatenate(byte[] arr1, byte[] arr2) {
        final int len1;
        final int len2;
        if (arr1 == null || (len1 = arr1.length) == 0) {
            return arr2 == null ? Constants.EMPTY_BYTE_ARRAY : arr2;
        } else if (arr2 == null || (len2 = arr2.length) == 0) {
            return arr1;
        }

        byte[] bytes = new byte[len1 + len2];
        System.arraycopy(arr1, 0, bytes, 0, len1);
        System.arraycopy(arr2, 0, bytes, len1, len2);
        return bytes;
    }

    static final JwtAlgorithm createAlgorithm(String secret) {
        final List<String> parts;
        if (Checker.isNullOrEmpty(secret)) {
            log.warn(
                    "Please set '%s' to a secure, randomly generated key to protect access tokens. You can create one using the command: \"openssl rand -base64 66 | tr -d '\\n'\".",
                    Option.SERVER_SECRET.getJdbcxName());
            parts = Collections.emptyList();
        } else {
            parts = new ArrayList<>(2);
            int index = secret.indexOf(':');
            if (index != -1) {
                parts.add(secret.substring(0, index));
                parts.add(secret.substring(index + 1));
            } else {
                parts.add(secret);
            }
        }

        final String algorithmName;
        if (parts.isEmpty()) {
            return new JwtAlgorithm(Jwts.SIG.NONE, null);
        } else if (parts.size() < 2) {
            algorithmName = "";
        } else {
            algorithmName = parts.remove(0).toUpperCase(Locale.ROOT);
            secret = parts.remove(0);
        }

        final SecretKey key = Keys.hmacShaKeyFor(secret.getBytes(Constants.DEFAULT_CHARSET));
        final JwtAlgorithm algorithm;
        switch (algorithmName) {
            case "HS512":
                algorithm = new JwtAlgorithm(Jwts.SIG.HS512, key);
                break;
            case "HS384":
                algorithm = new JwtAlgorithm(Jwts.SIG.HS384, key);
                break;
            case "HS256":
            default:
                algorithm = new JwtAlgorithm(Jwts.SIG.HS256, key);
                break;

        }
        return algorithm;
    }

    private final JwtAlgorithm secretAlg;
    private final AtomicReference<JwtParser> verifierRef;
    private final Path secretKeyFile;
    private final int keySizeBits;
    private final int ivLengthBytes;
    private final int tagLengthBits;
    private final String transformationNames;
    private final String algorithmName;
    private final Function<byte[], AlgorithmParameterSpec> paramSpecFunc;

    protected ConfigManager(Properties props) {
        this.secretAlg = createAlgorithm(Option.SERVER_SECRET.getJdbcxValue(props));
        this.verifierRef = new AtomicReference<>(null);
        this.secretKeyFile = Utils.getPath(OPTION_KEY_FILE.getJdbcxValue(props), true);
        this.keySizeBits = Integer.parseInt(OPTION_KEY_SIZE_BITS.getJdbcxValue(props));
        this.ivLengthBytes = Integer.parseInt(OPTION_IV_LENGTH_BYTES.getJdbcxValue(props));
        this.tagLengthBits = Integer.parseInt(OPTION_TAG_LENGTH_BITS.getJdbcxValue(props));

        this.transformationNames = OPTION_ALGORITHM.getJdbcxValue(props).trim();

        switch (transformationNames) {
            case ALGORITHM_AES_GCM_NOPADDING:
                this.algorithmName = ALGORITHM_AES;
                this.paramSpecFunc = iv -> new GCMParameterSpec(tagLengthBits, iv);
                break;
            case ALGORITHM_CHACHA20_POLY1305:
                this.algorithmName = ALGORITHM_CHACHA20;
                this.paramSpecFunc = IvParameterSpec::new;
                break;
            default:
                throw new IllegalArgumentException(
                        Utils.format("Unsupported algorithm [%s], please use either %s or %s.", transformationNames,
                                ALGORITHM_AES_GCM_NOPADDING, ALGORITHM_CHACHA20_POLY1305));
        }
    }

    protected final Key loadKey(Path keyFile, String algorithm) {
        if (keyFile == null) {
            keyFile = secretKeyFile;
        }
        if (Files.exists(keyFile)) {
            if (Checker.isNullOrEmpty(algorithm)) {
                algorithm = algorithmName;
            }
            try {
                return new SecretKeySpec(
                        Utils.fromBase64(Stream.readAllBytes(new FileInputStream(keyFile.toFile())), true),
                        algorithm);
            } catch (IOException e) {
                throw new IllegalArgumentException(Utils.format("Failed to load secret file [%s]", keyFile), e);
            }
        } else {
            throw new IllegalArgumentException(Utils.format("Missing secret file [%s]", keyFile));
        }
    }

    protected Key generateKeySpec(String algorithm, int keyBits) {
        if (Checker.isNullOrEmpty(algorithm)) {
            algorithm = algorithmName;
        }

        if (keyBits <= 0) {
            keyBits = keySizeBits;
        }

        try {
            KeyGenerator keyGen = KeyGenerator.getInstance(algorithm);
            keyGen.init(keyBits, random);
            return keyGen.generateKey();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException(e);
        }
    }

    protected final Key loadKey() {
        return loadKey(secretKeyFile, algorithmName);
    }

    protected final String generateKey() {
        return generateKey(algorithmName, keySizeBits);
    }

    protected final Cipher getCipherForDecryption(Key key, AlgorithmParameterSpec params)
            throws GeneralSecurityException {
        return getCipher(Cipher.DECRYPT_MODE, transformationNames, key, params);
    }

    protected final Cipher getCipherForEncyption(Key key, AlgorithmParameterSpec params)
            throws GeneralSecurityException {
        return getCipher(Cipher.ENCRYPT_MODE, transformationNames, key, params);
    }

    protected Cipher getCipher(int opmode, String algTransformationNames, Key key, AlgorithmParameterSpec params)
            throws GeneralSecurityException {
        if (Checker.isNullOrEmpty(algTransformationNames)) {
            algTransformationNames = transformationNames;
        }
        if (params == null) {
            byte[] iv = new byte[ivLengthBytes];
            random.nextBytes(iv); // Generate a unique IV for each encryption
            params = paramSpecFunc.apply(iv);
        }
        Cipher cipher = Cipher.getInstance(algTransformationNames);
        cipher.init(opmode, key != null ? key : loadKey(), params);
        return cipher;
    }

    protected String encrypt(Key key, String text, String associatedData, Charset charset) {
        if (charset == null) {
            charset = Constants.DEFAULT_CHARSET;
        }

        try {
            Cipher cipher = getCipherForEncyption(key, null);
            if (!Checker.isNullOrEmpty(associatedData)) {
                cipher.updateAAD(associatedData.getBytes(charset));
            }
            return Utils.toBase64(concatenate(cipher.getIV(), cipher.doFinal(text.getBytes(charset))));
        } catch (GeneralSecurityException e) {
            throw new IllegalArgumentException(e);
        }
    }

    protected String decrypt(Key key, String encryptedText, String associatedData, Charset charset) {
        if (charset == null) {
            charset = Constants.DEFAULT_CHARSET;
        }

        byte[] decodedBytes = Utils.fromBase64(encryptedText, charset, true);

        byte[] iv = new byte[ivLengthBytes];
        System.arraycopy(decodedBytes, 0, iv, 0, ivLengthBytes);

        byte[] ciphertextWithTag = new byte[decodedBytes.length - ivLengthBytes];
        System.arraycopy(decodedBytes, ivLengthBytes, ciphertextWithTag, 0, ciphertextWithTag.length);

        try {
            Cipher cipher = getCipherForDecryption(key, paramSpecFunc.apply(iv));
            if (!Checker.isNullOrEmpty(associatedData)) {
                cipher.updateAAD(associatedData.getBytes(charset));
            }
            return new String(cipher.doFinal(ciphertextWithTag), charset);
        } catch (GeneralSecurityException e) {
            throw new IllegalArgumentException(e);
        }
    }

    protected final void decrypt(Key key, Properties props, String associatedData, Charset charset) {
        if (props == null || props.isEmpty()) {
            return;
        }

        for (String name : props.stringPropertyNames()) {
            if (name != null && name.endsWith(PROPERTY_ENCRYPTED_SUFFIX)) {
                Object value = props.remove(name);
                if (value == null) {
                    continue;
                }
                props.setProperty(name.substring(0, name.length() - PROPERTY_ENCRYPTED_SUFFIX.length()),
                        decrypt(key, (String) value, associatedData, charset));
            }
        }
    }

    protected final String getUniqueId(String category, String id) {
        if (category == null) {
            category = Constants.EMPTY_STRING;
        }
        if (id == null) {
            id = Constants.EMPTY_STRING;
        }
        return new StringBuilder(category.length() + id.length() + 1).append(category).append('/').append(id)
                .toString();
    }

    protected final JwtParser getTokenVerifier(String issuer) {
        JwtParserBuilder builder = Jwts.parser();
        if (secretAlg.key == null) {
            builder = builder.unsecured();
        } else {
            builder = builder.verifyWith(secretAlg.key);
        }
        return builder.requireIssuer(issuer).build();
    }

    public String generateToken(String issuer, String subject, String audience, int expirationMinutes,
            Map<String, String> claims) {
        if (Checker.isNullOrBlank(issuer) || Checker.isNullOrBlank(subject)) {
            throw new IllegalArgumentException("Non-blank issuer and subject are required");
        }

        Instant now = Instant.now();
        JwtBuilder builder = Jwts.builder().issuer(issuer.trim()).subject(subject.trim())
                .id(UUID.randomUUID().toString()).issuedAt(Date.from(now));
        List<String> list = Utils.split(audience, ',', true, true, true);
        if (!list.isEmpty()) {
            builder = builder.audience().add(list).and();
        }
        if (expirationMinutes > 0) {
            builder = builder.expiration(Date.from(now.plus(expirationMinutes, ChronoUnit.MINUTES)));
        }
        if (claims != null) {
            for (Entry<String, String> e : claims.entrySet()) {
                String key = e.getKey();
                String val = e.getValue();
                if (!Checker.isNullOrBlank(key) && !Checker.isNullOrBlank(val)) {
                    builder = builder.claim(key.trim(), val.trim());
                }
            }
        }
        if (secretAlg.key != null) {
            builder = builder.signWith(secretAlg.key, secretAlg.alg);
        }
        return builder.compact();
    }

    public Map<String, String> verifyToken(String issuer, String token) {
        JwtParser verifier = verifierRef.get();
        // TODO use cache if we have multiple issuers
        if (verifier == null) {
            verifierRef.compareAndSet(null, verifier = getTokenVerifier(issuer));
        }

        try {
            Claims jwt = secretAlg.key == null ? verifier.parseUnsecuredClaims(token).getPayload()
                    : verifier.parseSignedClaims(token).getPayload();
            Map<String, String> claims = new HashMap<>();
            for (Entry<String, Object> e : jwt.entrySet()) {
                claims.put(e.getKey(), e.getValue() == null ? Constants.EMPTY_STRING : e.getValue().toString());
            }
            return Collections.unmodifiableMap(claims);
        } catch (IllegalArgumentException | JwtException e) {
            log.warn("JWT verification failed: %s", e.getMessage());
        }
        return Collections.emptyMap();
    }

    public List<String> getAllIDs(String category) { // NOSONAR
        return Collections.emptyList();
    }

    public List<String> getMatchedIDs(String category, String globPattern) {
        final Pattern pattern = cache.get(globPattern);
        if (pattern == null) {
            return Collections.emptyList();
        }

        List<String> ids = new LinkedList<>();
        for (String id : getAllIDs(category)) {
            if (pattern.matcher(id).matches()) {
                ids.add(id);
            }
        }
        return ids.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(ids));
    }

    public boolean hasConfig(String category, String id) { // NOSONAR
        return false;
    }

    public final Properties getConfig(String category, String id) {
        return getConfig(category, id, null, null);
    }

    public Properties getConfig(String category, String id, VariableTag tag, String tenant) {
        throw new UnsupportedOperationException();
    }

    public Properties load(String fileName, Properties base) {
        Properties props = loadConfig(fileName, null, base);
        Path file = Utils.getPath(fileName, true).getFileName();
        decrypt(props, file != null ? file.toString() : null);
        return props;
    }

    public void reload(Properties props) {
    }

    public final String generateKey(String algorithm, int keyBits) {
        return Utils.toBase64(generateKeySpec(algorithm, keyBits).getEncoded());
    }

    public final String encrypt(String text) {
        return encrypt(text, null, null);
    }

    public final String encrypt(String text, String associatedData, Charset charset) {
        return encrypt(null, text, associatedData, charset);
    }

    public final String decrypt(String encryptedText) {
        return decrypt(encryptedText, null, null);
    }

    public final String decrypt(String encryptedText, String associatedData, Charset charset) {
        return decrypt(null, encryptedText, associatedData, charset);
    }

    public final void decrypt(Properties props, String associatedData) {
        decrypt(null, props, associatedData, null);
    }

    public final void applySecrets(String tenant, VariableTag tag, Properties props) {
        if (Checker.isNullOrEmpty(tenant) || props == null) {
            return;
        }

        final Properties vars = tenants.get(tenant);
        if (vars != null && vars.size() > 0) {
            for (Entry<Object, Object> e : props.entrySet()) {
                String key = (String) e.getKey();
                String val = (String) e.getValue();
                String newVal = Utils.applyVariables(val, tag, vars);
                if (val != newVal) { // NOSONAR
                    props.setProperty(key, newVal);
                }
            }
        }
    }

    public final void register(String tenant, Properties secrets) {
        if (tenant == null || secrets == null || secrets.size() == 0) {
            throw new IllegalArgumentException("Non-null tenant and secrets are required");
        }

        tenants.put(tenant, secrets);
    }
}
