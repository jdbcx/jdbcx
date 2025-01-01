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
package io.github.jdbcx.security;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.KeyFactory;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import io.github.jdbcx.Checker;
import io.github.jdbcx.Option;
import io.github.jdbcx.Utils;

public class SslContextProvider {
    private static final SslContextProvider instance = new SslContextProvider();

    static final String PEM_HEADER_PREFIX = "---BEGIN ";
    static final String PEM_HEADER_SUFFIX = " PRIVATE KEY---";
    static final String PEM_FOOTER_PREFIX = "---END ";

    /**
     * An insecure {@link javax.net.ssl.TrustManager}, that never validates the
     * certificate.
     */
    static class NonValidatingTrustManager implements X509TrustManager {
        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }

        @Override
        @SuppressWarnings("squid:S4830")
        public void checkClientTrusted(X509Certificate[] certs, String authType) {
            // ignore
        }

        @Override
        @SuppressWarnings("squid:S4830")
        public void checkServerTrusted(X509Certificate[] certs, String authType) {
            // ignore
        }
    }

    static String getAlgorithm(String header, String defaultAlg) {
        int startIndex = header.indexOf(PEM_HEADER_PREFIX);
        int endIndex = startIndex < 0 ? startIndex
                : header.indexOf(PEM_HEADER_SUFFIX, (startIndex += PEM_HEADER_PREFIX.length()));
        return startIndex < endIndex ? header.substring(startIndex, endIndex) : defaultAlg;
    }

    static PrivateKey getPrivateKey(String keyFile, String keyAlg)
            throws NoSuchAlgorithmException, InvalidKeySpecException, IOException {
        String algorithm = Checker.isNullOrEmpty(keyAlg) ? Option.SSL_KEY_ALGORITHM.getDefaultValue() : keyAlg;
        StringBuilder builder = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(Utils.getFileInputStream(keyFile)))) {
            String line = reader.readLine();
            if (line != null) {
                algorithm = getAlgorithm(line, algorithm);

                while ((line = reader.readLine()) != null) {
                    if (line.indexOf(PEM_FOOTER_PREFIX) >= 0) {
                        break;
                    }

                    builder.append(line);
                }
            }
        }
        byte[] encoded = Base64.getDecoder().decode(builder.toString());
        KeyFactory kf = KeyFactory.getInstance(algorithm);
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(encoded);
        return kf.generatePrivate(keySpec);
    }

    protected KeyStore getKeyStore(String cert, String key, String certType, String keyAlg)
            throws NoSuchAlgorithmException, InvalidKeySpecException, IOException, CertificateException,
            KeyStoreException {
        final KeyStore ks;
        try {
            ks = KeyStore.getInstance(KeyStore.getDefaultType());
            ks.load(null, null); // needed to initialize the key store
        } catch (KeyStoreException e) {
            throw new NoSuchAlgorithmException(
                    Utils.format("%s KeyStore not available", KeyStore.getDefaultType()));
        }

        try (InputStream in = Utils.getFileInputStream(cert)) {
            CertificateFactory factory = CertificateFactory
                    .getInstance(Checker.isNullOrEmpty(certType) ? Option.SSL_CERT_TYPE.getDefaultValue() : certType);
            if (key == null || key.isEmpty()) {
                int index = 1;
                for (Certificate c : factory.generateCertificates(in)) {
                    ks.setCertificateEntry("cert" + (index++), c);
                }
            } else {
                Certificate[] certChain = factory.generateCertificates(in).toArray(new Certificate[0]);
                ks.setKeyEntry("key", getPrivateKey(key, keyAlg), null, certChain);
            }
        }
        return ks;
    }

    /**
     * Get non-null SSL context provider.
     * 
     * @return non-null SSL context provider
     */
    public static final SslContextProvider getProvider() {
        String packageName = SslContextProvider.class.getName();
        packageName = packageName.substring(0, packageName.lastIndexOf('.') + 1);
        SslContextProvider defaultProvider = null;
        for (SslContextProvider s : ServiceLoader.load(SslContextProvider.class,
                SslContextProvider.class.getClassLoader())) {
            if (s == null) {
                // impossible
            } else if (s.getClass().getName().startsWith(packageName)) {
                defaultProvider = s;
            } else {
                return s;
            }
        }
        return defaultProvider != null ? defaultProvider : instance;
    }

    protected SSLContext getJavaSslContext(Properties props) throws SSLException {
        String sslMode = Option.SSL_MODE.getValue(props);
        String clientCert = Option.SSL_CERT.getValue(props);
        String clientKey = Option.SSL_KEY.getValue(props);
        String sslRootCert = Option.SSL_ROOT_CERT.getValue(props);

        SSLContext ctx;
        try {
            ctx = SSLContext.getInstance(Option.SSL_PROTOCOL.getValue(props));
            TrustManager[] tms = null;
            KeyManager[] kms = null;
            SecureRandom sr = null;

            if ("none".equalsIgnoreCase(sslMode)) {
                tms = new TrustManager[] { new NonValidatingTrustManager() };
                kms = new KeyManager[0];
                sr = new SecureRandom();
            } else if ("strict".equalsIgnoreCase(sslMode)) {
                if (clientCert != null && !clientCert.isEmpty()) {
                    KeyManagerFactory factory = KeyManagerFactory
                            .getInstance(KeyManagerFactory.getDefaultAlgorithm());
                    factory.init(getKeyStore(clientCert, clientKey, Option.SSL_CERT_TYPE.getValue(props),
                            Option.SSL_KEY_ALGORITHM.getValue(props)), null);
                    kms = factory.getKeyManagers();
                }

                if (sslRootCert != null && !sslRootCert.isEmpty()) {
                    TrustManagerFactory factory = TrustManagerFactory
                            .getInstance(TrustManagerFactory.getDefaultAlgorithm());
                    factory.init(getKeyStore(sslRootCert, null, Option.SSL_CERT_TYPE.getValue(props),
                            Option.SSL_KEY_ALGORITHM.getValue(props)));
                    tms = factory.getTrustManagers();
                }
                sr = new SecureRandom();
            } else {
                throw new IllegalArgumentException(Utils.format("unspported ssl mode '%s'", sslMode));
            }

            ctx.init(kms, tms, sr);
        } catch (KeyManagementException | InvalidKeySpecException | NoSuchAlgorithmException | KeyStoreException
                | CertificateException | IOException | UnrecoverableKeyException e) {
            throw new SSLException("Failed to get SSL context", e);
        }

        return ctx;
    }

    @SuppressWarnings("unchecked")
    public <T> Optional<T> getSslContext(Class<? extends T> sslContextClass, Properties config) throws SSLException {
        return SSLContext.class == sslContextClass ? Optional.of((T) getJavaSslContext(config)) : Optional.empty();
    }
}
