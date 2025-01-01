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

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ExpandedUrlClassLoaderTest {
    @Test(groups = "unit")
    public void testLoadRemoteDriver() throws Exception {
        final String remoteFile = "https://repo1.maven.org/maven2/org/slf4j/slf4j-jdk14/2.0.6/slf4j-jdk14-2.0.6.jar";
        final String classToLoad = "org.slf4j.jul.JDK14LoggerFactory";
        ClassLoader loader = getClass().getClassLoader();
        Assert.assertThrows(ClassNotFoundException.class, () -> loader.loadClass(classToLoad));

        // load class from remote URL
        try (ExpandedUrlClassLoader customLoader = (ExpandedUrlClassLoader) ExpandedUrlClassLoader.of(loader,
                remoteFile)) {
            Assert.assertNotNull(customLoader.loadClass(classToLoad));
        }
        Assert.assertThrows(ClassNotFoundException.class, () -> loader.loadClass(classToLoad));

        Path localFile = Files.createTempFile("class", ".jar");
        try (InputStream in = Utils.toURL(remoteFile).openStream()) {
            Files.copy(in, localFile, StandardCopyOption.REPLACE_EXISTING);
        }
        // load class from specific file in local file system
        try (ExpandedUrlClassLoader customLoader = (ExpandedUrlClassLoader) ExpandedUrlClassLoader.of(loader,
                localFile.toFile().getAbsolutePath())) {
            Assert.assertNotNull(customLoader.loadClass(classToLoad));
        }
        // load class from local directory
        Assert.assertThrows(ClassNotFoundException.class, () -> loader.loadClass(classToLoad));
        try (ExpandedUrlClassLoader customLoader = (ExpandedUrlClassLoader) ExpandedUrlClassLoader.of(loader,
                localFile.getParent().toFile().getAbsolutePath())) {
            Assert.assertNotNull(customLoader.loadClass(classToLoad));
        }
        Assert.assertThrows(ClassNotFoundException.class, () -> loader.loadClass(classToLoad));
    }
}
