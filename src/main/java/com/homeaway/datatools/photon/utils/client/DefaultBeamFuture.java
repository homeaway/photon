/* Copyright (c) 2019 Expedia Group.
 * All rights reserved.  http://www.homeaway.com

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *      http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.homeaway.datatools.photon.utils.client;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.homeaway.datatools.photon.api.beam.BeamFuture;
import com.homeaway.datatools.photon.dao.beam.WriteFuture;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DefaultBeamFuture implements BeamFuture {

    private final ListenableFuture<List<Boolean>> future;
    private final List<WriteFuture> futures;

    public DefaultBeamFuture(final List<WriteFuture> writeFutures) {
        this.future = Futures.allAsList(writeFutures);
        this.futures = writeFutures;
    }

    public DefaultBeamFuture(final WriteFuture writeFuture) {
        this(Lists.newArrayList(writeFuture));
    }

    @Override
    public void addListener(Runnable listener, Executor executor) {
        future.addListener(listener, executor);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return future.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return future.isCancelled();
    }

    @Override
    public boolean isDone() {
        return futures.stream()
                .map(WriteFuture::isDone)
                .allMatch(d -> d == Boolean.TRUE);
    }

    @Override
    public Boolean get() throws InterruptedException, ExecutionException {
        return !future.get()
                .stream()
                .anyMatch(b -> b == Boolean.FALSE);
    }

    @Override
    public Boolean get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return !future.get(timeout, unit)
                .stream()
                .anyMatch(b -> Boolean.FALSE);
    }
}
