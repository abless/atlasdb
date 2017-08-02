/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.timelock.lock;

import com.palantir.common.time.Clock;

public class LeaseExpirationTimer {

    public static final long LEASE_TIMEOUT_MILLIS = 20_000;

    private volatile long lastRefreshTimeMillis;
    private final Clock clock;

    public LeaseExpirationTimer(Clock clock) {
        this.clock = clock;
        this.lastRefreshTimeMillis = clock.getTimeMillis();
    }

    public void refresh() {
        lastRefreshTimeMillis = clock.getTimeMillis();
    }

    public boolean isExpired() {
        return clock.getTimeMillis() > lastRefreshTimeMillis + LEASE_TIMEOUT_MILLIS;
    }

}