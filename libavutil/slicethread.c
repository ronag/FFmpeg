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
#include "cpu.h"
#include "internal.h"
#include "slicethread.h"
#include "mem.h"
#include "thread.h"
#include "avassert.h"

#define MAX_AUTO_THREADS 16

#if HAVE_PTHREADS || HAVE_W32THREADS || HAVE_OS2THREADS

typedef struct WorkerContext {
    AVSliceThread   *ctx;
    pthread_mutex_t mutex;
    pthread_cond_t  cond;
    pthread_t       thread;
    int             done;
} WorkerContext;

struct AVSliceThread {
    WorkerContext   *workers;
    int             nb_threads;
    int             nb_active_threads;
    int             nb_jobs;

    atomic_uint     first_job;
    atomic_uint     current_job;
    pthread_mutex_t done_mutex;
    pthread_cond_t  done_cond;
    int             done;
    int             finished;
    atomic_int      error;

    void            *priv;
    int            (*worker_func)(void *priv, int jobnr, int threadnr, int nb_jobs, int nb_threads);
    int            (*main_func)(void *priv);

#if LIBAVUTIL_VERSION_MAJOR < 62
    void           (*worker_func_v1)(void *priv, int jobnr, int threadnr, int nb_jobs, int nb_threads);
    void           (*main_func_v1)(void *priv);
    void            *priv_v1;
#endif

    /* Contexts on the shared pool, which also use nb_jobs, current_job,
     * error and done_cond. The rest is under pool.lock. */
    int             shared;
    int             nb_lanes;
    int             next_lane;      ///< next lane a pool thread joins
    int             nb_joined;      ///< pool threads still running a lane
    int             queued;         ///< waiting in pool.queue for threads
    AVSliceThread  *queue_next;
    int            *lane_thread;    ///< per lane, the pool thread that last ran it, or -1
};

/*
 * The process-wide slice thread pool.
 *
 * An execute hands lanes 1 and up to idle pool threads and runs lane 0 on
 * the calling thread. Lanes it cannot hand out wait in the queue for a
 * thread that finishes other work. Every lane claims jobs in jobnr order
 * from the same counter, so the calling thread alone can finish an execute
 * that gets no pool thread at all. Nothing ever waits for a pool thread to
 * become free, only for jobs that a running thread has claimed, which keeps
 * the pool free of deadlocks, also when a job executes a nested context
 * (see AVPRIV_SLICETHREAD_FLAG_SHARED for what jobs may wait on).
 *
 * Idle threads form a stack, so the thread that was busy last, whose cache
 * is warmest, runs next. A context asks for the thread that ran each lane
 * last time, so state a job keeps per threadnr stays in that thread's cache.
 * Threads are not pinned to CPUs: the scheduler places them better than a
 * fixed mapping can on a shared, cpuset-limited host.
 *
 * The pool also has one run slot per thread. A lane runs only with a slot,
 * and so do threads of their own that enter the pool, like frame threads,
 * for each frame. A slot holder that waits for another holder lends its slot
 * (avpriv_slicethread_pool_block()), and takes it back when it resumes
 * without waiting, which may briefly run the pool over its size. So a
 * thread waits for a slot only before it starts work, and every slot holder
 * either runs, or waits for something that a running thread or a thread
 * outside the pool does. Threads waiting to enter get slots before lanes,
 * because a lane's execute finishes without them.
 */
typedef struct PoolThread {
    pthread_t       thread;
    pthread_cond_t  cond;
    AVSliceThread  *ctx;            ///< whose lane it runs, under pool.lock
    int             lane;
    int             idle_pos;       ///< index in pool.idle, or -1 while busy
} PoolThread;

static struct {
    pthread_mutex_t lock;
    PoolThread     *threads;
    int             nb_threads;
    int            *idle;           ///< idle thread indices, most recently busy last
    int             nb_idle;
    AVSliceThread  *queue;          ///< executes with lanes left, oldest first
    int             nb_active;      ///< run slots in all, see av_slice_thread_pool_set_active()
    int             slots;          ///< run slots left, below 0 while resumed threads run over
    int             nb_slot_waiters;
    pthread_cond_t  slot_cond;
} pool;
static atomic_int pool_ready;
static AVMutex pool_init_lock = AV_MUTEX_INITIALIZER;

/* Whether this thread holds a run slot, or lent it while it waits. */
static _Thread_local int slot_held, slot_lent;

static int auto_threads(int nb_threads)
{
    int nb_cpus;

    av_assert0(nb_threads >= 0);
    if (nb_threads)
        return nb_threads;
    nb_cpus = av_cpu_count();
    return nb_cpus > 1 ? FFMIN(nb_cpus + 1, MAX_AUTO_THREADS) : 1;
}

static void pool_run_lane(AVSliceThread *ctx, int lane)
{
    unsigned nb_jobs = ctx->nb_jobs, job;

    while ((job = atomic_fetch_add_explicit(&ctx->current_job, 1, memory_order_relaxed)) < nb_jobs) {
        int ret = atomic_load_explicit(&ctx->error, memory_order_relaxed);
        if (ret)
            continue;
        ret = ctx->worker_func(ctx->priv, job, lane, nb_jobs, ctx->nb_lanes);
        if (ret) {
            int prev = 0;
            atomic_compare_exchange_strong_explicit(&ctx->error, &prev, ret,
                                                    memory_order_relaxed,
                                                    memory_order_relaxed);
        }
    }
}

/* Whether a lane may take a run slot. */
static int pool_slot_free(void)
{
    return pool.slots > 0 && !pool.nb_slot_waiters;
}

static void pool_join(PoolThread *t, AVSliceThread *ctx)
{
    t->ctx  = ctx;
    t->lane = ctx->next_lane++;
    ctx->lane_thread[t->lane] = t - pool.threads;
    ctx->nb_joined++;
    pool.slots--;
}

static void pool_queue_remove(AVSliceThread *ctx)
{
    AVSliceThread **p = &pool.queue;

    while (*p != ctx)
        p = &(*p)->queue_next;
    *p = ctx->queue_next;
    ctx->queued = 0;
}

/* Joins the oldest queued execute that still has jobs and lanes left. */
static int pool_join_queued(PoolThread *t)
{
    if (!pool_slot_free())
        return 0;
    while (pool.queue) {
        AVSliceThread *ctx = pool.queue;

        if (atomic_load_explicit(&ctx->current_job, memory_order_relaxed) >= (unsigned)ctx->nb_jobs) {
            pool_queue_remove(ctx);
            continue;
        }
        pool_join(t, ctx);
        if (ctx->next_lane == ctx->nb_lanes)
            pool_queue_remove(ctx);
        return 1;
    }
    return 0;
}

/* Gives a run slot back: to a thread waiting to enter, or else to an idle
 * thread for a queued execute if dispatch is set. */
static void pool_put_slot(int dispatch)
{
    if (++pool.slots <= 0)
        return;
    if (pool.nb_slot_waiters) {
        pthread_cond_signal(&pool.slot_cond);
    } else if (dispatch && pool.nb_idle) {
        PoolThread *t = &pool.threads[pool.idle[pool.nb_idle - 1]];

        if (pool_join_queued(t)) {
            pool.nb_idle--;
            t->idle_pos = -1;
            pthread_cond_signal(&t->cond);
        }
    }
}

static void *attribute_align_arg pool_thread(void *arg)
{
    PoolThread *t = arg;

    pthread_mutex_lock(&pool.lock);
    while (1) {
        AVSliceThread *ctx;
        int lane;

        while (!t->ctx) {
            if (t->idle_pos < 0 && !pool_join_queued(t)) {
                t->idle_pos = pool.nb_idle;
                pool.idle[pool.nb_idle++] = t - pool.threads;
            }
            if (!t->ctx)
                pthread_cond_wait(&t->cond, &pool.lock);
        }
        ctx  = t->ctx;
        lane = t->lane;
        pthread_mutex_unlock(&pool.lock);

        slot_held = 1;
        pool_run_lane(ctx, lane);
        slot_held = 0;

        pthread_mutex_lock(&pool.lock);
        t->ctx = NULL;
        if (!--ctx->nb_joined)
            pthread_cond_signal(&ctx->done_cond);
        /* This thread looks for queued lanes itself. */
        pool_put_slot(0);
    }
    return NULL;
}

static int pool_execute(AVSliceThread *ctx, int nb_jobs)
{
    ctx->nb_jobs  = nb_jobs;
    ctx->nb_lanes = FFMIN(nb_jobs, ctx->nb_threads);
    atomic_store_explicit(&ctx->current_job, 0, memory_order_relaxed);
    atomic_store_explicit(&ctx->error, 0, memory_order_relaxed);

    if (ctx->nb_lanes > 1) {
        int handed_out;

        pthread_mutex_lock(&pool.lock);
        ctx->next_lane = 1;
        while (ctx->next_lane < ctx->nb_lanes && pool.nb_idle && pool_slot_free()) {
            int last = ctx->lane_thread[ctx->next_lane];
            int idx  = last >= 0 && pool.threads[last].idle_pos >= 0 ? last
                                                                    : pool.idle[pool.nb_idle - 1];
            PoolThread *t = &pool.threads[idx];

            for (int i = t->idle_pos; i < pool.nb_idle - 1; i++) {
                pool.idle[i] = pool.idle[i + 1];
                pool.threads[pool.idle[i]].idle_pos = i;
            }
            pool.nb_idle--;
            t->idle_pos = -1;

            pool_join(t, ctx);
        }
        handed_out = ctx->next_lane;
        if (ctx->next_lane < ctx->nb_lanes) {
            AVSliceThread **p = &pool.queue;
            while (*p)
                p = &(*p)->queue_next;
            *p = ctx;
            ctx->queue_next = NULL;
            ctx->queued = 1;
        }
        pthread_mutex_unlock(&pool.lock);

        /* Woken under the lock, each thread would block on it right away. */
        for (int lane = 1; lane < handed_out; lane++)
            pthread_cond_signal(&pool.threads[ctx->lane_thread[lane]].cond);
    }

    pool_run_lane(ctx, 0);

    if (ctx->nb_lanes > 1) {
        int lend;

        pthread_mutex_lock(&pool.lock);
        if (ctx->queued)
            pool_queue_remove(ctx);
        /* A slot holder executing a nested context waits for other holders. */
        lend = slot_held && ctx->nb_joined;
        if (lend) {
            slot_held = 0;
            pool_put_slot(1);
        }
        while (ctx->nb_joined)
            pthread_cond_wait(&ctx->done_cond, &pool.lock);
        if (lend) {
            pool.slots--;
            slot_held = 1;
        }
        pthread_mutex_unlock(&pool.lock);
    }

    return atomic_load_explicit(&ctx->error, memory_order_relaxed);
}

int av_slice_thread_pool_init(int nb_threads)
{
    int ret = 0;

    if (nb_threads < 0)
        return AVERROR(EINVAL);
    if (!nb_threads)
        nb_threads = av_cpu_count();

    ff_mutex_lock(&pool_init_lock);
    if (atomic_load_explicit(&pool_ready, memory_order_relaxed)) {
        ret = AVERROR(EEXIST);
        goto end;
    }

    pool.threads = av_calloc(nb_threads, sizeof(*pool.threads));
    pool.idle    = av_calloc(nb_threads, sizeof(*pool.idle));
    if (!pool.threads || !pool.idle) {
        ret = AVERROR(ENOMEM);
        goto fail;
    }
    if ((ret = pthread_mutex_init(&pool.lock, NULL))) {
        ret = AVERROR(ret);
        goto fail;
    }
    if ((ret = pthread_cond_init(&pool.slot_cond, NULL))) {
        pthread_mutex_destroy(&pool.lock);
        ret = AVERROR(ret);
        goto fail;
    }

    pthread_mutex_lock(&pool.lock);
    for (int i = 0; i < nb_threads; i++) {
        PoolThread *t = &pool.threads[i];

        t->idle_pos = -1;
        if ((ret = pthread_cond_init(&t->cond, NULL)))
            break;
        if ((ret = pthread_create(&t->thread, NULL, pool_thread, t))) {
            pthread_cond_destroy(&t->cond);
            break;
        }
        pool.nb_threads++;
    }
    pool.slots = pool.nb_active = pool.nb_threads;
    pthread_mutex_unlock(&pool.lock);

    /* With some threads started, the pool works with fewer. */
    if (!pool.nb_threads) {
        pthread_cond_destroy(&pool.slot_cond);
        pthread_mutex_destroy(&pool.lock);
        ret = AVERROR(ret);
        goto fail;
    }
    ret = 0;
    atomic_store_explicit(&pool_ready, 1, memory_order_release);
    goto end;

fail:
    av_freep(&pool.threads);
    av_freep(&pool.idle);
end:
    ff_mutex_unlock(&pool_init_lock);
    return ret;
}

int av_slice_thread_pool_set_active(int nb_active)
{
    if (nb_active < 1)
        return AVERROR(EINVAL);
    if (!atomic_load_explicit(&pool_ready, memory_order_acquire))
        return AVERROR(EINVAL);

    pthread_mutex_lock(&pool.lock);
    /* Fewer slots take effect as holders give theirs back. */
    if (nb_active < pool.nb_active)
        pool.slots -= pool.nb_active - nb_active;
    else
        for (int i = pool.nb_active; i < nb_active; i++)
            pool_put_slot(1);
    pool.nb_active = nb_active;
    pthread_mutex_unlock(&pool.lock);
    return 0;
}

void avpriv_slicethread_pool_enter(void)
{
    if (slot_held || !atomic_load_explicit(&pool_ready, memory_order_acquire))
        return;

    pthread_mutex_lock(&pool.lock);
    pool.nb_slot_waiters++;
    while (pool.slots <= 0)
        pthread_cond_wait(&pool.slot_cond, &pool.lock);
    pool.nb_slot_waiters--;
    pool.slots--;
    if (pool.slots > 0 && pool.nb_slot_waiters)
        pthread_cond_signal(&pool.slot_cond);
    pthread_mutex_unlock(&pool.lock);
    slot_held = 1;
}

void avpriv_slicethread_pool_leave(void)
{
    if (!slot_held)
        return;
    slot_held = 0;
    pthread_mutex_lock(&pool.lock);
    pool_put_slot(1);
    pthread_mutex_unlock(&pool.lock);
}

void avpriv_slicethread_pool_block(void)
{
    if (!slot_held)
        return;
    slot_held = 0;
    slot_lent = 1;
    pthread_mutex_lock(&pool.lock);
    pool_put_slot(1);
    pthread_mutex_unlock(&pool.lock);
}

void avpriv_slicethread_pool_unblock(void)
{
    if (!slot_lent)
        return;
    slot_lent = 0;
    slot_held = 1;
    pthread_mutex_lock(&pool.lock);
    pool.slots--;
    pthread_mutex_unlock(&pool.lock);
}

static int run_jobs(AVSliceThread *ctx)
{
    unsigned nb_jobs    = ctx->nb_jobs;
    unsigned nb_active_threads = ctx->nb_active_threads;
    unsigned first_job    = atomic_fetch_add_explicit(&ctx->first_job, 1, memory_order_acq_rel);
    unsigned current_job  = first_job;

    do {
        int ret = atomic_load_explicit(&ctx->error, memory_order_relaxed);
        if (ret)
            continue;
        ret = ctx->worker_func(ctx->priv, current_job, first_job, nb_jobs, nb_active_threads);
        if (ret) {
            int prev = 0;
            atomic_compare_exchange_strong_explicit(&ctx->error, &prev, ret,
                                                    memory_order_relaxed,
                                                    memory_order_relaxed);
        }
    } while ((current_job = atomic_fetch_add_explicit(&ctx->current_job, 1, memory_order_acq_rel)) < nb_jobs);

    return current_job == nb_jobs + nb_active_threads - 1;
}

static void *attribute_align_arg thread_worker(void *v)
{
    WorkerContext *w = v;
    AVSliceThread *ctx = w->ctx;

    pthread_mutex_lock(&w->mutex);
    pthread_cond_signal(&w->cond);

    while (1) {
        w->done = 1;
        while (w->done)
            pthread_cond_wait(&w->cond, &w->mutex);

        if (ctx->finished) {
            pthread_mutex_unlock(&w->mutex);
            return NULL;
        }

        if (run_jobs(ctx)) {
            pthread_mutex_lock(&ctx->done_mutex);
            ctx->done = 1;
            pthread_cond_signal(&ctx->done_cond);
            pthread_mutex_unlock(&ctx->done_mutex);
        }
    }
}

av_cold
int avpriv_slicethread_create2(AVSliceThread **pctx, void *priv,
                               int (*worker_func)(void *priv, int jobnr, int threadnr, int nb_jobs, int nb_threads),
                               int (*main_func)(void *priv),
                               int nb_threads)
{
    AVSliceThread *ctx;
    int nb_workers, i;
    int ret;

    nb_threads = auto_threads(nb_threads);
    nb_workers = nb_threads;
    if (!main_func)
        nb_workers--;

    *pctx = ctx = av_mallocz(sizeof(*ctx));
    if (!ctx)
        return AVERROR(ENOMEM);

    if (nb_workers && !(ctx->workers = av_calloc(nb_workers, sizeof(*ctx->workers)))) {
        av_freep(pctx);
        return AVERROR(ENOMEM);
    }

    ctx->priv        = priv;
    ctx->worker_func = worker_func;
    ctx->main_func   = main_func;
    ctx->nb_threads  = nb_threads;
    ctx->nb_active_threads = 0;
    ctx->nb_jobs     = 0;
    ctx->finished    = 0;

    atomic_init(&ctx->first_job, 0);
    atomic_init(&ctx->current_job, 0);
    ret = pthread_mutex_init(&ctx->done_mutex, NULL);
    if (ret) {
        av_freep(&ctx->workers);
        av_freep(pctx);
        return AVERROR(ret);
    }
    ret = pthread_cond_init(&ctx->done_cond, NULL);
    if (ret) {
        ctx->nb_threads = main_func ? 0 : 1;
        avpriv_slicethread_free(pctx);
        return AVERROR(ret);
    }
    ctx->done        = 0;

    for (i = 0; i < nb_workers; i++) {
        WorkerContext *w = &ctx->workers[i];
        w->ctx = ctx;
        ret = pthread_mutex_init(&w->mutex, NULL);
        if (ret) {
            ctx->nb_threads = main_func ? i : i + 1;
            avpriv_slicethread_free(pctx);
            return AVERROR(ret);
        }
        ret = pthread_cond_init(&w->cond, NULL);
        if (ret) {
            pthread_mutex_destroy(&w->mutex);
            ctx->nb_threads = main_func ? i : i + 1;
            avpriv_slicethread_free(pctx);
            return AVERROR(ret);
        }
        pthread_mutex_lock(&w->mutex);
        w->done = 0;

        if (ret = pthread_create(&w->thread, NULL, thread_worker, w)) {
            ctx->nb_threads = main_func ? i : i + 1;
            pthread_mutex_unlock(&w->mutex);
            pthread_cond_destroy(&w->cond);
            pthread_mutex_destroy(&w->mutex);
            avpriv_slicethread_free(pctx);
            return AVERROR(ret);
        }

        while (!w->done)
            pthread_cond_wait(&w->cond, &w->mutex);
        pthread_mutex_unlock(&w->mutex);
    }

    return nb_threads;
}

av_cold
int avpriv_slicethread_create3(AVSliceThread **pctx, void *priv,
                               int (*worker_func)(void *priv, int jobnr, int threadnr, int nb_jobs, int nb_threads),
                               int (*main_func)(void *priv),
                               int nb_threads, unsigned flags)
{
    AVSliceThread *ctx;
    int ret;

    if (!(flags & AVPRIV_SLICETHREAD_FLAG_SHARED) || main_func ||
        !atomic_load_explicit(&pool_ready, memory_order_acquire))
        return avpriv_slicethread_create2(pctx, priv, worker_func, main_func, nb_threads);

    nb_threads = auto_threads(nb_threads);
    *pctx = ctx = av_mallocz(sizeof(*ctx));
    if (!ctx)
        return AVERROR(ENOMEM);
    ctx->lane_thread = av_malloc_array(nb_threads, sizeof(*ctx->lane_thread));
    if (!ctx->lane_thread) {
        av_freep(pctx);
        return AVERROR(ENOMEM);
    }
    for (int i = 0; i < nb_threads; i++)
        ctx->lane_thread[i] = -1;
    if ((ret = pthread_cond_init(&ctx->done_cond, NULL))) {
        av_freep(&ctx->lane_thread);
        av_freep(pctx);
        return AVERROR(ret);
    }

    ctx->shared      = 1;
    ctx->priv        = priv;
    ctx->worker_func = worker_func;
    ctx->nb_threads  = nb_threads;
    atomic_init(&ctx->current_job, 0);
    atomic_init(&ctx->error, 0);
    return nb_threads;
}

int avpriv_slicethread_execute2(AVSliceThread *ctx, int nb_jobs, int execute_main)
{
    int nb_workers, i, is_last = 0, ret = 0;

    av_assert0(nb_jobs > 0);
    if (ctx->shared)
        return pool_execute(ctx, nb_jobs);

    ctx->nb_jobs           = nb_jobs;
    ctx->nb_active_threads = FFMIN(nb_jobs, ctx->nb_threads);
    atomic_store_explicit(&ctx->error, 0, memory_order_relaxed);
    atomic_store_explicit(&ctx->first_job, 0, memory_order_relaxed);
    atomic_store_explicit(&ctx->current_job, ctx->nb_active_threads, memory_order_relaxed);
    nb_workers             = ctx->nb_active_threads;
    if (!ctx->main_func || !execute_main)
        nb_workers--;

    for (i = 0; i < nb_workers; i++) {
        WorkerContext *w = &ctx->workers[i];
        pthread_mutex_lock(&w->mutex);
        w->done = 0;
        pthread_cond_signal(&w->cond);
        pthread_mutex_unlock(&w->mutex);
    }

    if (ctx->main_func && execute_main) {
        ret = ctx->main_func(ctx->priv);
    } else
        is_last = run_jobs(ctx);

    if (!is_last) {
        pthread_mutex_lock(&ctx->done_mutex);
        while (!ctx->done)
            pthread_cond_wait(&ctx->done_cond, &ctx->done_mutex);
        ctx->done = 0;
        pthread_mutex_unlock(&ctx->done_mutex);
    }

    if (!ret)
        ret = atomic_load_explicit(&ctx->error, memory_order_relaxed);

    return ret;
}

av_cold void avpriv_slicethread_free(AVSliceThread **pctx)
{
    AVSliceThread *ctx = *pctx;
    int nb_workers, i;

    if (!ctx)
        return;

    if (ctx->shared) {
        pthread_cond_destroy(&ctx->done_cond);
        av_freep(&ctx->lane_thread);
        av_freep(pctx);
        return;
    }

    nb_workers = ctx->nb_threads;
    if (!ctx->main_func)
        nb_workers--;

    ctx->finished = 1;
    for (i = 0; i < nb_workers; i++) {
        WorkerContext *w = &ctx->workers[i];
        pthread_mutex_lock(&w->mutex);
        w->done = 0;
        pthread_cond_signal(&w->cond);
        pthread_mutex_unlock(&w->mutex);
    }

    for (i = 0; i < nb_workers; i++) {
        WorkerContext *w = &ctx->workers[i];
        pthread_join(w->thread, NULL);
        pthread_cond_destroy(&w->cond);
        pthread_mutex_destroy(&w->mutex);
    }

    pthread_cond_destroy(&ctx->done_cond);
    pthread_mutex_destroy(&ctx->done_mutex);
    av_freep(&ctx->workers);
    av_freep(pctx);
}

/**
 * Backwards compatibility wrapper for the deprecated avpriv_ slicethread API.
 */

#if LIBAVUTIL_VERSION_MAJOR < 62

static int wrapper_worker(void *priv, int jobnr, int threadnr, int nb_jobs, int nb_threads)
{
    AVSliceThread *ctx = priv;
    ctx->worker_func_v1(ctx->priv_v1, jobnr, threadnr, nb_jobs, nb_threads);
    return 0;
}

static int wrapper_main(void *priv)
{
    AVSliceThread *ctx = priv;
    ctx->main_func_v1(ctx->priv_v1);
    return 0;
}

int avpriv_slicethread_create(AVSliceThread **pctx, void *priv,
                              void (*worker_func)(void *priv, int jobnr, int threadnr, int nb_jobs, int nb_threads),
                              void (*main_func)(void *priv),
                              int nb_threads)
{
    int ret = avpriv_slicethread_create2(pctx, NULL, wrapper_worker,
                                         main_func ? wrapper_main : NULL,
                                         nb_threads);
    if (ret < 0)
        return ret;

    (*pctx)->priv           = *pctx;
    (*pctx)->priv_v1        = priv;
    (*pctx)->worker_func_v1 = worker_func;
    (*pctx)->main_func_v1   = main_func;
    return ret;
}

void avpriv_slicethread_execute(AVSliceThread *ctx, int nb_jobs, int execute_main)
{
    avpriv_slicethread_execute2(ctx, nb_jobs, execute_main);
}

#endif /* LIBAVUTIL_VERSION_MAJOR < 62 */

#else /* HAVE_PTHREADS || HAVE_W32THREADS || HAVE_OS32THREADS */

int avpriv_slicethread_create2(AVSliceThread **pctx, void *priv,
                               int (*worker_func)(void *priv, int jobnr, int threadnr, int nb_jobs, int nb_threads),
                               int (*main_func)(void *priv),
                               int nb_threads)
{
    *pctx = NULL;
    return AVERROR(ENOSYS);
}

int avpriv_slicethread_create3(AVSliceThread **pctx, void *priv,
                               int (*worker_func)(void *priv, int jobnr, int threadnr, int nb_jobs, int nb_threads),
                               int (*main_func)(void *priv),
                               int nb_threads, unsigned flags)
{
    *pctx = NULL;
    return AVERROR(ENOSYS);
}

int av_slice_thread_pool_init(int nb_threads)
{
    return AVERROR(ENOSYS);
}

int av_slice_thread_pool_set_active(int nb_active)
{
    return AVERROR(ENOSYS);
}

void avpriv_slicethread_pool_enter(void) {}
void avpriv_slicethread_pool_leave(void) {}
void avpriv_slicethread_pool_block(void) {}
void avpriv_slicethread_pool_unblock(void) {}

int avpriv_slicethread_execute2(AVSliceThread *ctx, int nb_jobs, int execute_main)
{
    av_assert0(0);
}

void avpriv_slicethread_free(AVSliceThread **pctx)
{
    av_assert0(!pctx || !*pctx);
}

#if LIBAVUTIL_VERSION_MAJOR < 62

int avpriv_slicethread_create(AVSliceThread **pctx, void *priv,
                              void (*worker_func)(void *priv, int jobnr, int threadnr, int nb_jobs, int nb_threads),
                              void (*main_func)(void *priv),
                              int nb_threads)
{
    *pctx = NULL;
    return AVERROR(ENOSYS);
}

void avpriv_slicethread_execute(AVSliceThread *ctx, int nb_jobs, int execute_main)
{
    av_assert0(0);
}

#endif /* LIBAVUTIL_VERSION_MAJOR < 62 */

#endif /* HAVE_PTHREADS || HAVE_W32THREADS || HAVE_OS32THREADS */
