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

public enum Format {
    // https://www.iana.org/assignments/media-types/text/csv
    CSV("text/csv", "csv"),
    // https://www.iana.org/assignments/media-types/text/tab-separated-values
    TSV("text/tab-separated-values", "tsv"),
    TXT("text/plain", "txt"),
    // https://www.iana.org/assignments/media-types/application/json
    JSON("application/json", "json"),
    // https://www.iana.org/assignments/media-types/application/xml
    XML("application/xml", "xml"),
    // https://www.iana.org/assignments/media-types/application/vnd.apache.arrow.file
    ARROW("application/vnd.apache.arrow.file", "arrow"),
    // non-standard
    // https://avro.apache.org/docs/1.11.1/specification/#encodings
    AVRO("avro/binary", "avro"), // https://avro.apache.org/docs/1.11.1/specification/#http-as-transport
    AVRO_BINARY("application/vnd.apache.avro+binary", "avrob"),
    AVRO_JSON("application/vnd.apache.avro+json", "avroj"),
    BSON("application/bson", "bson"),
    JSONL("application/jsonl", "jsonl"), // or application/json-lines?
    NDJSON("application/x-ndjson", "ndjson"), // or application/json-seq?
    // https://issues.apache.org/jira/browse/PARQUET-1889
    PARQUET("application/vnd.apache.parquet", "parquet");

    /**
     * Get preferred data format based on given MIME types.
     *
     * @param mimeTypes     acceptable MIME types, for example:
     *                      {@code text/html, application/xhtml+xml, application/xml;q=0.9, image/webp, *_/_*;q=0.8}
     * @param defaultFormat optional default format, {@code null} is same as
     *                      {@link #CSV}
     * @return non-null data format
     */
    public static Format fromMimeType(String mimeTypes, Format defaultFormat) {
        if (defaultFormat == null) {
            defaultFormat = CSV;
        }

        for (String part : Utils.split(mimeTypes, ',')) {
            for (String str : Utils.split(part.trim(), ';')) {
                String type = str.trim().toLowerCase(Locale.ROOT);
                if (type.isEmpty() || type.charAt(0) == '*') {
                    return defaultFormat;
                } else if (type.charAt(type.length() - 1) == '*') {
                    type = Utils.split(type, '/').get(0).trim();
                    for (Format f : values()) {
                        if (f.mimeType.startsWith(type)) {
                            return f;
                        }
                    }
                } else {
                    for (Format f : values()) {
                        if (f.mimeType.equals(type)) {
                            return f;
                        }
                    }
                }
            }
        }
        return defaultFormat;
    }

    /**
     * Get format based on given file name.
     *
     * @param file file name
     * @return format, could be {@code null}
     */
    public static Format fromFileName(String file) {
        String ext = null;
        int index = 0;
        if (file != null && (index = file.lastIndexOf('.')) > 0) {
            ext = file.substring(index + 1);
        }

        return fromFileExtension(ext, null); // NOSONAR
    }

    /**
     * Get format based on given file extension.
     *
     * @param ext           file extension without dot prefix
     * @param defaultFormat default format
     * @return {@code defaultFormat} if file extension is unknown
     */
    public static Format fromFileExtension(String ext, Format defaultFormat) {
        Format format = defaultFormat;

        if (!Checker.isNullOrEmpty(ext)) {
            ext = ext.toLowerCase(Locale.ROOT);
            for (Format c : values()) {
                if (c.fileExt.equals(ext)) {
                    format = c;
                    break;
                }
            }
        }

        return format;
    }

    /**
     * Normalizes the given field name used in Avro/Parquet data format.
     *
     * @param index 1-based index
     * @param name  original field name which may or may not be valid according to
     *              Avro spec
     * @return normalized field name with invalid characters removed and
     *         {@code f<index>_} prefix
     */
    public static String normalizeAvroField(int index, String name) {
        if (index < 1) {
            index = 1;
        }
        final int len;
        if (name == null || (len = name.length()) < 1) {
            return new StringBuilder().append('f').append(index).toString();
        }

        StringBuilder builder = new StringBuilder(len + 3);
        // start with [A-Za-z_]
        char ch = name.charAt(0);
        if (ch == '_' || (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z')) {
            builder.append(ch);
        } else {
            builder.append('f').append(index).append('_');
            if (ch >= '0' && ch <= '9') {
                builder.append(ch);
            }
        }
        // subsequently contain only [A-Za-z0-9_]
        for (int i = 1; i < len; i++) {
            ch = name.charAt(i);
            if (ch == '_' || (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9')) {
                builder.append(ch);
            }
        }
        int lastIndex = builder.length() - 1;
        if (builder.charAt(lastIndex) == '_') {
            builder.setLength(lastIndex);
        }
        return builder.toString();
    }

    private String mimeType;
    private String fileExt;
    private String fileExtWithDot;

    Format(String mimeType, String fileExt) {
        this.mimeType = mimeType;
        this.fileExt = fileExt;
        this.fileExtWithDot = ".".concat(fileExt);
    }

    public String mimeType() {
        return mimeType;
    }

    public String fileExtension() {
        return fileExtension(true);
    }

    public String fileExtension(boolean withDot) {
        return withDot ? fileExtWithDot : fileExt;
    }
}
