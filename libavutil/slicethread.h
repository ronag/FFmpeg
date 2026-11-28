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

#ifndef AVUTIL_SLICETHREAD_H
#define AVUTIL_SLICETHREAD_H

#include "attributes.h"
#include "version.h"

typedef struct AVSliceThread AVSliceThread;

/**
 * Create slice threading context.
 * @param pctx slice threading context returned here
 * @param priv private pointer to be passed to callback function
 * @param worker_func callback function to be executed
 * @param main_func special callback function, called from main thread, may be NULL
 * @param nb_threads number of threads, 0 for automatic, must be >= 0
 * @return return number of threads or negative AVERROR on failure
 */
int avpriv_slicethread_create2(AVSliceThread **pctx, void *priv,
                               int (*worker_func)(void *priv, int jobnr, int threadnr, int nb_jobs, int nb_threads),
                               int (*main_func)(void *priv),
                               int nb_threads);

/**
 * The jobs of one execute may run on the process-wide slice thread pool
 * (av_slice_thread_pool_init()) instead of threads of the context's own.
 *
 * On the pool, an execute may get fewer threads than nb_threads, down to
 * just the calling thread, which claims jobs in jobnr order until none are
 * left. So a job may wait on a job with a lower jobnr of the same execute,
 * but never on one with a higher jobnr, and never on a job of another
 * execute. threadnr still stays below nb_threads and is unique among the
 * jobs of one execute that run at the same time.
 *
 * Ignored with a main_func, which runs alongside the jobs and so needs
 * threads of its own.
 */
#define AVPRIV_SLICETHREAD_FLAG_SHARED (1 << 0)

/**
 * Wait for a run slot of the shared slice thread pool, and hold it until
 * avpriv_slicethread_pool_leave(). The pool runs at most as many slot
 * holders at once as it has slots, its slice lanes included. For threads of
 * their own that do the same kind of work, like frame threads for each
 * frame. Returns at once without the pool, or if the thread holds a slot.
 */
void avpriv_slicethread_pool_enter(void);

/**
 * Give back the run slot taken by avpriv_slicethread_pool_enter().
 */
void avpriv_slicethread_pool_leave(void);

/**
 * Bracket a wait that another slot holder may have to end, such as waiting
 * for another frame's progress: the slot is free for others meanwhile.
 * avpriv_slicethread_pool_unblock() takes it back without waiting, so a
 * thread that resumes never waits for a slot, and the pool may run over its
 * size until holders leave or block. No-ops for a thread without a slot.
 */
void avpriv_slicethread_pool_block(void);
void avpriv_slicethread_pool_unblock(void);

/**
 * Like avpriv_slicethread_create2(), with AVPRIV_SLICETHREAD_FLAG_* flags.
 */
int avpriv_slicethread_create3(AVSliceThread **pctx, void *priv,
                               int (*worker_func)(void *priv, int jobnr, int threadnr, int nb_jobs, int nb_threads),
                               int (*main_func)(void *priv),
                               int nb_threads, unsigned flags);

/**
 * Execute slice threading. If any job returns a nonzero value,
 * all remaining jobs will be aborted and that value returned.
 *
 * If `main_func` itself returns a nonzero value, that value is guaranteed
 * to be returned, even if a worker thread returns a nonzero value first.
 *
 * @param ctx slice threading context
 * @param nb_jobs number of jobs, must be > 0
 * @param execute_main also execute main_func
 * @return 0 on success, else first seen return value
 */
int avpriv_slicethread_execute2(AVSliceThread *ctx, int nb_jobs, int execute_main);

/**
 * Destroy slice threading context.
 * @param pctx pointer to context
 */
void avpriv_slicethread_free(AVSliceThread **pctx);

#if LIBAVUTIL_VERSION_MAJOR < 62
/**
 * @deprecated
 * Wrapper around avpriv_slicethread_create2() for ABI backwards compatibility.
 */
attribute_deprecated
int avpriv_slicethread_create(AVSliceThread **pctx, void *priv,
                              void (*worker_func)(void *priv, int jobnr, int threadnr, int nb_jobs, int nb_threads),
                              void (*main_func)(void *priv),
                              int nb_threads);

/**
 * @deprecated
 * Wrapper around avpriv_slicethread_execute2() for ABI backwards compatibility.
 */
attribute_deprecated
void avpriv_slicethread_execute(AVSliceThread *ctx, int nb_jobs, int execute_main);
#endif /* LIBAVUTIL_VERSION_MAJOR < 62 */

#endif
