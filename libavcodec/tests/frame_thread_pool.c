/*
 * This file is part of FFmpeg.
 *
 * FFmpeg is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * FFmpeg is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with FFmpeg; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 */


#include <stdatomic.h>
#include <stdio.h>

#include "libavcodec/pthread_frame.c"
#include "libavutil/cpu.h"
#include "libavutil/time.h"

static FrameThreadContext decoder = { .async_lock = 1 };
static atomic_int entered, finished;

static void *hw_worker(void *arg)
{
    avpriv_thread_pool_enter();
    atomic_store(&entered, 1);
    async_lock(&decoder);
    async_unlock(&decoder);
    avpriv_thread_pool_leave();
    return NULL;
}

static void *other_worker(void *arg)
{
    avpriv_thread_pool_enter();
    avpriv_thread_pool_leave();
    atomic_store(&finished, 1);
    return NULL;
}

static int wait_for(atomic_int *flag)
{
    int64_t start = av_gettime_relative();
    while (!atomic_load(flag)) {
        if (av_gettime_relative() - start > 10000000) {
            fprintf(stderr, "Timed out waiting for the frame worker\n");
            return 1;
        }
        av_usleep(1000);
    }
    return 0;
}

int main(void)
{
    pthread_t hw, other;

    /* While the caller owns the hw decoder's async lock, another decoder
     * in the same process must be able to use the only pool slot. */
    if (pthread_mutex_init(&decoder.async_mutex, NULL) ||
        pthread_cond_init(&decoder.async_cond, NULL) ||
        av_thread_pool_init(1) < 0 ||
        pthread_create(&hw, NULL, hw_worker, NULL) || wait_for(&entered) ||
        pthread_create(&other, NULL, other_worker, NULL) || wait_for(&finished))
        return 1;

    async_unlock(&decoder);
    pthread_join(other, NULL);
    pthread_join(hw, NULL);
    pthread_cond_destroy(&decoder.async_cond);
    pthread_mutex_destroy(&decoder.async_mutex);
    return 0;
}
