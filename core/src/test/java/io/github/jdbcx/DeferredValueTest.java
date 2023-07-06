/*
 * Copyright 2022-2023, Zhichun Wu
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.testng.Assert;
import org.testng.annotations.Test;

public class DeferredValueTest {
    @Test(groups = { "unit" })
    public void testDeferredValues() throws Exception {
        final List<Integer> list = new ArrayList<>(2);
        DeferredValue<?> v = DeferredValue.of(list, List.class);
        Assert.assertEquals(v.get(), list);
        list.add(3);
        Assert.assertEquals(v.get(), list);

        v = DeferredValue.of(() -> {
            list.add(5);
            return list;
        });
        Assert.assertEquals(list, Arrays.asList(3));
        Assert.assertEquals(v.get(), list);
        Assert.assertEquals(list, Arrays.asList(3, 5));

        CountDownLatch latch = new CountDownLatch(1);
        v = DeferredValue.of(CompletableFuture.supplyAsync(() -> {
            try {
                latch.await(3000L, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            list.remove(0);
            return list;
        }), 500L);
        Assert.assertEquals(list, Arrays.asList(3, 5));
        latch.countDown();
        Thread.sleep(1000L);
        Assert.assertEquals(v.get(), list);
        Assert.assertEquals(list, Arrays.asList(5));
    }

    @Test(groups = { "unit" })
    public void testNullValues() {
        Assert.assertNull(DeferredValue.of((CompletableFuture<?>) null).get());
        Assert.assertNull(DeferredValue.of((CompletableFuture<?>) null, 50L).get());

        Assert.assertNull(DeferredValue.of((Supplier<?>) null).get());

        Assert.assertNull(DeferredValue.of(null, Object.class).get());
        Assert.assertNull(DeferredValue.of(null, null).get());
    }
}
