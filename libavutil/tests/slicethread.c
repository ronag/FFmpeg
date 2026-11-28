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

/*
 * Tests slice threading with threads of the context's own and on the shared
 * pool: every job runs once, threadnr stays in range and unique among the
 * jobs running at once, errors propagate, and jobs that wait on jobs with a
 * lower jobnr finish with more callers than pool threads, also when a job
 * executes a nested context. Threads of their own that enter the pool get
 * no more run slots at once than it has active threads, and newer "frames"
 * that take every slot and then wait on older ones still finish, since
 * waiting lends the slot. A watchdog turns a deadlock into a failure.
 */

#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "libavutil/cpu.h"
#include "libavutil/error.h"
#include "libavutil/macros.h"
#include "libavutil/slicethread.h"
#include "libavutil/thread.h"
#include "libavutil/time.h"

#define MAX_JOBS    64
#define MAX_THREADS 8
#define NB_CALLERS  6

typedef struct Test {
    AVSliceThread  *slicethread;
    int             nb_threads;
    int             nb_jobs;
    int             chain;          ///< each job waits on the one before it
    int             ring;           ///< each job waits for the next one to start
    int             fail_job;
    struct Test    *nested;         ///< per threadnr, executed by each job
    atomic_int      runs[MAX_JOBS];
    atomic_int      started[MAX_JOBS];
    atomic_int      done[MAX_JOBS];
    atomic_int      lane_busy[MAX_THREADS];
    atomic_int      main_ran;
    AVMutex         lock;
    AVCond          cond;
} Test;

static atomic_int failed;
static atomic_int finished;

#define CHECK(cond) do {                                                      \
    if (!(cond)) {                                                            \
        fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, #cond); \
        atomic_store(&failed, 1);                                             \
    }                                                                         \
} while (0)

static void set_flag(Test *t, atomic_int *flag)
{
    ff_mutex_lock(&t->lock);
    atomic_store(flag, 1);
    ff_cond_broadcast(&t->cond);
    ff_mutex_unlock(&t->lock);
}

static void wait_flag(Test *t, atomic_int *flag)
{
    ff_mutex_lock(&t->lock);
    while (!atomic_load(flag))
        ff_cond_wait(&t->cond, &t->lock);
    ff_mutex_unlock(&t->lock);
}

static int run(Test *t, int nb_jobs);

static int worker(void *priv, int jobnr, int threadnr, int nb_jobs, int nb_threads)
{
    Test *t = priv;

    CHECK(nb_jobs == t->nb_jobs);
    CHECK(nb_threads == FFMIN(t->nb_jobs, t->nb_threads));
    CHECK(threadnr >= 0 && threadnr < nb_threads);
    CHECK(!atomic_fetch_add(&t->lane_busy[threadnr], 1));
    atomic_fetch_add(&t->runs[jobnr], 1);
    set_flag(t, &t->started[jobnr]);

    if (t->chain && jobnr)
        wait_flag(t, &t->done[jobnr - 1]);
    if (t->ring)
        wait_flag(t, &t->started[(jobnr + 1) % nb_jobs]);
    if (t->nested)
        CHECK(!run(&t->nested[threadnr], MAX_JOBS));

    atomic_fetch_sub(&t->lane_busy[threadnr], 1);
    set_flag(t, &t->done[jobnr]);
    return jobnr == t->fail_job ? AVERROR(EIO) : 0;
}

static int main_func(void *priv)
{
    Test *t = priv;

    for (int i = 0; i < t->nb_jobs; i++)
        wait_flag(t, &t->done[i]);
    atomic_store(&t->main_ran, 1);
    return 0;
}

static int init(Test *t, int nb_threads, int with_main, unsigned flags)
{
    int ret;

    memset(t, 0, sizeof(*t));
    t->fail_job = -1;
    ff_mutex_init(&t->lock, NULL);
    ff_cond_init(&t->cond, NULL);
    ret = avpriv_slicethread_create3(&t->slicethread, t, worker,
                                     with_main ? main_func : NULL, nb_threads, flags);
    CHECK(ret == nb_threads);
    t->nb_threads = ret;
    return ret < 0 ? ret : 0;
}

static void uninit(Test *t)
{
    avpriv_slicethread_free(&t->slicethread);
    ff_cond_destroy(&t->cond);
    ff_mutex_destroy(&t->lock);
}

static int run(Test *t, int nb_jobs)
{
    int ret;

    t->nb_jobs = nb_jobs;
    for (int i = 0; i < nb_jobs; i++) {
        atomic_store(&t->runs[i], 0);
        atomic_store(&t->started[i], 0);
        atomic_store(&t->done[i], 0);
    }
    ret = avpriv_slicethread_execute2(t->slicethread, nb_jobs, 0);
    if (t->fail_job < 0)
        for (int i = 0; i < nb_jobs; i++)
            CHECK(atomic_load(&t->runs[i]) == 1);
    return ret;
}

/* Each caller executes jobs that wait on the job before them and each run a
 * nested context, on its own context. */
static void *caller(void *arg)
{
    Test t, nested[MAX_THREADS];

    if (init(&t, MAX_THREADS, 0, AVPRIV_SLICETHREAD_FLAG_SHARED) < 0)
        return NULL;
    for (int i = 0; i < MAX_THREADS; i++)
        init(&nested[i], 4, 0, AVPRIV_SLICETHREAD_FLAG_SHARED);
    t.chain = 1;
    t.nested = nested;
    for (int i = 0; i < 20; i++)
        CHECK(!run(&t, 1 + i * 3 % MAX_JOBS));
    for (int i = 0; i < MAX_THREADS; i++)
        uninit(&nested[i]);
    uninit(&t);
    return NULL;
}

#define NB_FRAMES 8

static atomic_int running, max_running;
static atomic_int frame_done[NB_FRAMES];
static AVMutex frame_lock = AV_MUTEX_INITIALIZER;
static AVCond  frame_cond;

/* Holds a run slot a few times, counting how many threads hold one at once. */
static void *slot_user(void *arg)
{
    for (int i = 0; i < 20; i++) {
        int n;

        avpriv_slicethread_pool_enter();
        n = atomic_fetch_add(&running, 1) + 1;
        for (int m = atomic_load(&max_running); n > m && !atomic_compare_exchange_weak(&max_running, &m, n);)
            ;
        av_usleep(200);
        atomic_fetch_sub(&running, 1);
        avpriv_slicethread_pool_leave();
    }
    return NULL;
}

static int max_slot_users(void)
{
    pthread_t threads[8];

    atomic_store(&max_running, 0);
    for (int i = 0; i < FF_ARRAY_ELEMS(threads); i++)
        CHECK(!pthread_create(&threads[i], NULL, slot_user, NULL));
    for (int i = 0; i < FF_ARRAY_ELEMS(threads); i++)
        pthread_join(threads[i], NULL);
    return atomic_load(&max_running);
}

/* Like a frame thread: decodes frame n with a run slot, after waiting for
 * frame n - 1, and runs a nested execute whose jobs wait on each other. */
static void *frame(void *arg)
{
    int n = (intptr_t)arg;
    Test t;

    avpriv_slicethread_pool_enter();
    if (n) {
        avpriv_slicethread_pool_block();
        ff_mutex_lock(&frame_lock);
        while (!atomic_load(&frame_done[n - 1]))
            ff_cond_wait(&frame_cond, &frame_lock);
        ff_mutex_unlock(&frame_lock);
        avpriv_slicethread_pool_unblock();
    }
    if (!init(&t, 4, 0, AVPRIV_SLICETHREAD_FLAG_SHARED)) {
        t.chain = 1;
        CHECK(!run(&t, 16));
        uninit(&t);
    }
    ff_mutex_lock(&frame_lock);
    atomic_store(&frame_done[n], 1);
    ff_cond_broadcast(&frame_cond);
    ff_mutex_unlock(&frame_lock);
    avpriv_slicethread_pool_leave();
    return NULL;
}

static void *watchdog(void *arg)
{
    for (int i = 0; i < 600 && !atomic_load(&finished); i++)
        av_usleep(100000);
    if (!atomic_load(&finished)) {
        fprintf(stderr, "deadlock: not finished after 60 s\n");
        abort();
    }
    return NULL;
}

int main(void)
{
    pthread_t watchdog_thread, callers[NB_CALLERS];
    Test t;

    if (pthread_create(&watchdog_thread, NULL, watchdog, NULL))
        return 1;
    ff_cond_init(&frame_cond, NULL);

    /* Without the pool, run slots are no-ops. */
    avpriv_slicethread_pool_enter();
    avpriv_slicethread_pool_block();
    avpriv_slicethread_pool_unblock();
    avpriv_slicethread_pool_leave();
    CHECK(av_slice_thread_pool_set_active(1) == AVERROR(EINVAL));

    /* Before the pool exists, contexts get threads of their own and run all
     * jobs at once, so jobs may wait for any other job to start. */
    if (init(&t, 4, 0, AVPRIV_SLICETHREAD_FLAG_SHARED) < 0)
        return 1;
    t.ring = 1;
    CHECK(!run(&t, 4));
    uninit(&t);

    CHECK(av_slice_thread_pool_init(3) == 0);
    CHECK(av_slice_thread_pool_init(3) == AVERROR(EEXIST));

    /* A main_func runs alongside the jobs, so it keeps threads of its own. */
    if (init(&t, 4, 1, AVPRIV_SLICETHREAD_FLAG_SHARED) < 0)
        return 1;
    t.ring = 1;
    t.nb_jobs = 4;
    CHECK(!avpriv_slicethread_execute2(t.slicethread, 4, 1));
    CHECK(atomic_load(&t.main_ran));
    uninit(&t);

    if (init(&t, MAX_THREADS, 0, AVPRIV_SLICETHREAD_FLAG_SHARED) < 0)
        return 1;
    for (int i = 1; i <= MAX_JOBS; i++)
        CHECK(!run(&t, i));
    t.fail_job = 5;
    CHECK(run(&t, MAX_JOBS) == AVERROR(EIO));
    t.fail_job = -1;
    t.chain = 1;
    for (int i = 0; i < 50; i++)
        CHECK(!run(&t, MAX_JOBS));
    uninit(&t);

    for (int i = 0; i < NB_CALLERS; i++)
        CHECK(!pthread_create(&callers[i], NULL, caller, NULL));
    for (int i = 0; i < NB_CALLERS; i++)
        pthread_join(callers[i], NULL);

    CHECK(max_slot_users() <= 3);
    CHECK(av_slice_thread_pool_set_active(0) == AVERROR(EINVAL));
    CHECK(!av_slice_thread_pool_set_active(1));
    CHECK(max_slot_users() == 1);

    /* With one slot, newer frames start first and take it, then wait on older
     * frames that have not started. */
    {
        pthread_t frames[NB_FRAMES];
        for (int i = NB_FRAMES - 1; i >= 0; i--)
            CHECK(!pthread_create(&frames[i], NULL, frame, (void *)(intptr_t)i));
        for (int i = 0; i < NB_FRAMES; i++)
            pthread_join(frames[i], NULL);
        for (int i = 0; i < NB_FRAMES; i++)
            CHECK(atomic_load(&frame_done[i]));
    }
    CHECK(!av_slice_thread_pool_set_active(3));
    CHECK(max_slot_users() <= 3);

    atomic_store(&finished, 1);
    pthread_join(watchdog_thread, NULL);
    return atomic_load(&failed);
}
