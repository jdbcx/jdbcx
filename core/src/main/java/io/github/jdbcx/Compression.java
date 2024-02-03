/*
 * Copyright 2022-2024, Zhichun Wu
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

import java.util.Locale;

public enum Compression {
    NONE("", "identity", ""),
    BROTLI("application/x-brotli", "br", "br"),
    BZIP2("application/x-bzip2", "bz2", "bz2", (byte) 'B', (byte) 'Z', (byte) 'h'),
    DEFLATE("application/x-deflate", "deflate", "zz", (byte) 0x78),
    // https://www.iana.org/assignments/media-types/application/gzip
    GZIP("application/gzip", "gzip", "gz", (byte) 0x1F, (byte) 0x8B),
    LZ4("application/x-lz4", "lz4", "lz4", (byte) 0x04, (byte) 0x22, (byte) 0x4D, (byte) 0x18),
    SNAPPY("application/x-snappy", "snappy", "sz"),
    XZ("application/x-xz", "xz", "xz", (byte) 0xFD, (byte) 0x37, (byte) 0x7A, (byte) 0x58, (byte) 0x5A, (byte) 0x00),
    // https://www.iana.org/assignments/media-types/application/zstd
    ZSTD("application/zstd", "zstd", "zst", (byte) 0xFD, (byte) 0x2F, (byte) 0xB5, (byte) 0x28);

    private String mimeType;
    private String encoding;
    private String fileExt;
    private String fileExtWithDot;
    private byte[] magicBytes;

    // and maybe magic bytes?
    Compression(String mimeType, String encoding, String fileExt, byte... magicBytes) {
        this.mimeType = mimeType;
        this.encoding = encoding;
        this.fileExt = fileExt;
        this.fileExtWithDot = ".".concat(fileExt);
        this.magicBytes = magicBytes;
    }

    public String mimeType() {
        return mimeType;
    }

    public String encoding() {
        return encoding;
    }

    public String fileExtension() {
        return fileExtension(true);
    }

    public String fileExtension(boolean withDot) {
        return withDot ? fileExtWithDot : fileExt;
    }

    public boolean hasMagicNumber() {
        return magicBytes.length > 0;
    }

    /**
     * Get compression algorithm based on given MIME type.
     *
     * @param mimeTypes MIME type
     * @return compression algorithm
     */
    public static Compression fromMimeType(String mimeTypes) {
        final Compression defaultCompress = NONE;
        for (String part : Utils.split(mimeTypes, ',')) {
            for (String str : Utils.split(part.trim(), ';')) {
                String type = str.trim().toLowerCase(Locale.ROOT);
                if (type.isEmpty() || type.charAt(0) == '*') {
                    return defaultCompress;
                } else if (type.charAt(type.length() - 1) == '*') {
                    type = Utils.split(type, '/').get(0).trim();
                    for (Compression c : values()) {
                        if (c.mimeType.startsWith(type)) {
                            return c;
                        }
                    }
                } else {
                    for (Compression c : values()) {
                        if (c.mimeType.equals(type)) {
                            return c;
                        }
                    }
                }
            }
        }
        return defaultCompress;
    }

    /**
     * Get preferred compression algorithm based on given encodings.
     *
     * @param encodings acceptable encodings, for example:
     *                  {@code br;q=1.0, gzip;q=0.8, *;q=0.1}
     * @return non-null compression algorithm
     */
    public static Compression fromEncoding(String encodings) {
        for (String part : Utils.split(encodings, ',')) {
            for (String str : Utils.split(part.trim(), ';')) {
                final String enc = str.trim().toLowerCase(Locale.ROOT);
                if (enc.isEmpty()) {
                    return Compression.NONE;
                } else if (enc.charAt(0) == '*') {
                    // widly supported with less CPU and memory overhead
                    return Compression.GZIP;
                }

                for (Compression c : values()) {
                    if (c.encoding.equals(enc)) {
                        return c;
                    }
                }
            }
        }

        return NONE;
    }

    /**
     * Get compression algorithm based on given file name.
     *
     * @param file file name
     * @return compression algorithm, could be null
     */
    public static Compression fromFileName(String file) {
        String ext = null;
        int index = 0;
        if (!Checker.isNullOrEmpty(file) && (index = file.lastIndexOf('.')) > 0) {
            ext = file.substring(index + 1);
        }
        return fromFileExtension(ext, null);
    }

    public static Compression fromFileExtension(String ext, Compression defaultCompress) {
        Compression compression = defaultCompress;

        if (ext != null) {
            ext = ext.toLowerCase(Locale.ROOT);
            for (Compression c : values()) {
                if (c.fileExt.equals(ext)) {
                    compression = c;
                    break;
                }
            }
        }

        return compression;
    }

    public static Compression fromMagicNumber(byte[] bytes) {
        Compression compression = null;

        int len = bytes != null ? bytes.length : 0;
        if (len > 0) {
            outer_loop: for (Compression c : values()) {
                byte[] m = c.magicBytes;
                int l = m.length;
                if (l > 0 && l <= len) {
                    for (int i = 0; i < l; i++) {
                        if (m[i] != bytes[i]) {
                            continue outer_loop;
                        }
                    }
                    compression = c;
                    break;
                }
            }
        }
        return compression;
    }
}
