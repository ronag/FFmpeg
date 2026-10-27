/*
 * log functions
 * Copyright (c) 2003 Michel Bardiaux
 *
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

#include "libavutil/log.c"

#include <string.h>

#if HAVE_PTHREADS && HAVE_UNISTD_H
static const AVClass class_a = {
    .class_name = "A",
    .item_name  = av_default_item_name,
    .version    = LIBAVUTIL_VERSION_INT,
};
static const AVClass class_b = {
    .class_name = "B",
    .item_name  = av_default_item_name,
    .version    = LIBAVUTIL_VERSION_INT,
};
static struct { const AVClass *class; } ctx_a = { &class_a }, ctx_b = { &class_b };

static void *log_open_line(void *arg)
{
    av_log(&ctx_a, AV_LOG_INFO, "a-start");
    return NULL;
}

/* A line left open is continued only by the same context in the same thread;
 * any other message starts on a new line, with its own prefix. */
static int check_open_lines(void)
{
    char buf[1024], expected[1024];
    FILE *tmp = tmpfile();
    int saved_stderr = dup(STDERR_FILENO);
    int ret = 1;
    pthread_t thread;
    size_t len;

    if (!tmp || saved_stderr < 0) {
        if (tmp)
            fclose(tmp);
        if (saved_stderr >= 0)
            close(saved_stderr);
        return 1;
    }

    use_color = 0;
    fflush(stderr);
    if (dup2(fileno(tmp), STDERR_FILENO) < 0)
        goto restore_stderr;

    /* Another context in the same thread. */
    av_log(&ctx_a, AV_LOG_INFO, "a-start");
    av_log(&ctx_b, AV_LOG_INFO, "b-line\n");
    av_log(&ctx_a, AV_LOG_INFO, " a-end\n");
    /* The same context in another thread. */
    if (pthread_create(&thread, NULL, log_open_line, NULL) ||
        pthread_join(thread, NULL))
        goto restore_stderr;
    av_log(&ctx_a, AV_LOG_INFO, "a-line\n");
    /* The same context in the same thread continues its line. */
    av_log(&ctx_a, AV_LOG_INFO, "a-start");
    av_log(&ctx_a, AV_LOG_INFO, " a-end\n");
    ret = 0;

restore_stderr:
    fflush(stderr);
    if (dup2(saved_stderr, STDERR_FILENO) < 0)
        ret = 1;
    close(saved_stderr);
    if (ret) {
        fclose(tmp);
        return ret;
    }

    rewind(tmp);
    len = fread(buf, 1, sizeof(buf) - 1, tmp);
    buf[len] = 0;
    fclose(tmp);

    snprintf(expected, sizeof(expected),
             "[A @ %p] a-start\n"
             "[B @ %p] b-line\n"
             "[A @ %p]  a-end\n"
             "[A @ %p] a-start\n"
             "[A @ %p] a-line\n"
             "[A @ %p] a-start a-end\n",
             &ctx_a, &ctx_b, &ctx_a, &ctx_a, &ctx_a, &ctx_a);
    if (strcmp(buf, expected)) {
        printf("Test open lines failed, got:\n%s\nexpected:\n%s", buf, expected);
        return 1;
    }
    return 0;
}
#endif

static int call_log_format_line2(const char *fmt, char *buffer, int buffer_size, ...)
{
    va_list args;
    int ret;
    int print_prefix=1;
    va_start(args, buffer_size);
    ret = av_log_format_line2(NULL, AV_LOG_INFO, fmt, args, buffer, buffer_size, &print_prefix);
    va_end(args);
    return ret;
}

#if HAVE_THREADS
static void *log_thread_open_line(void *ctx)
{
    av_log(ctx, AV_LOG_INFO, "worker open");
    return NULL;
}

static void *log_thread(void *ctx)
{
    av_log(ctx, AV_LOG_INFO, "worker ");
    av_log(ctx, AV_LOG_INFO, "line\n");
    return NULL;
}
#endif

static int test_log_lines(int threaded)
{
    /* NULL classes keep the prefixes independent of context addresses. */
    const AVClass *contexts[2] = { NULL, NULL };

    av_log_set_level(AV_LOG_INFO);
    av_log_set_flags(AV_LOG_PRINT_LEVEL | AV_LOG_SKIP_REPEATED);
    check_color_terminal();
    use_color = 0;

    if (!threaded) {
        av_log(&contexts[0], AV_LOG_INFO, "same ");
        av_log(&contexts[0], AV_LOG_INFO, "context\n");
        av_log(&contexts[0], AV_LOG_INFO, "interrupted");
        av_log(&contexts[1], AV_LOG_INFO, "other context\n");
        av_log(&contexts[0], AV_LOG_INFO, "continuation\n");
        av_log(NULL, AV_LOG_INFO, "global");
        av_log(&contexts[0], AV_LOG_INFO, "context\n");
        av_log(NULL, AV_LOG_INFO, "global continuation\n");
        av_log(&contexts[0], AV_LOG_INFO, "filtered ");
        av_log(&contexts[1], AV_LOG_DEBUG, "hidden\n");
        av_log(&contexts[0], AV_LOG_INFO, "continuation\n");
        av_log(&contexts[0], AV_LOG_INFO, "repeated\n");
        av_log(&contexts[0], AV_LOG_INFO, "repeated\n");
        av_log(&contexts[1], AV_LOG_INFO, "after repeat\n");
    }
#if HAVE_THREADS
    else {
        pthread_t thread;
        int ret;

        for (int i = 0; i < 2; i++) {
            void *ctx = i ? NULL : &contexts[0];

            av_log(ctx, AV_LOG_INFO, "shared %s", i ? "global" : "context");
            ret = pthread_create(&thread, NULL, log_thread, ctx);
            if (ret)
                return 1;
            ret = pthread_join(thread, NULL);
            if (ret)
                return 1;
            av_log(ctx, AV_LOG_INFO, "main continuation\n");

            /* A new thread must not inherit a departed thread's open line,
             * even when the implementation reuses its native thread ID. */
            ret = pthread_create(&thread, NULL, log_thread_open_line, ctx);
            if (ret)
                return 1;
            ret = pthread_join(thread, NULL);
            if (ret)
                return 1;
            ret = pthread_create(&thread, NULL, log_thread, ctx);
            if (ret)
                return 1;
            ret = pthread_join(thread, NULL);
            if (ret)
                return 1;
        }
    }
#endif
    return 0;
}

int main(int argc, char **argv)
{
    int i;

    if (argc > 1)
        return test_log_lines(!strcmp(argv[1], "threads"));

    av_log_set_level(AV_LOG_DEBUG);
    check_color_terminal();
    for (use_color=0; use_color<=256; use_color = 255*use_color+1) {
        av_log(NULL, AV_LOG_FATAL, "use_color: %d\n", use_color);
        for (i = AV_LOG_DEBUG; i>=AV_LOG_QUIET; i-=8) {
            av_log(NULL, i, " %d", i);
            av_log(NULL, AV_LOG_INFO, "e ");
            av_log(NULL, i + 256*123, "C%d", i);
            av_log(NULL, AV_LOG_INFO, "e");
        }
        av_log(NULL, AV_LOG_PANIC, "\n");
    }
    {
        int result;
        char buffer[4];
        result = call_log_format_line2("foo", NULL, 0);
        if(result != 3) {
            printf("Test NULL buffer failed.\n");
            return 1;
        }
        result = call_log_format_line2("foo", buffer, 2);
        if(result != 3 || strncmp(buffer, "f", 2)) {
            printf("Test buffer too small failed.\n");
            return 1;
        }
        result = call_log_format_line2("foo", buffer, 4);
        if(result != 3 || strncmp(buffer, "foo", 4)) {
            printf("Test buffer sufficiently big failed.\n");
            return 1;
        }
    }
#if HAVE_PTHREADS && HAVE_UNISTD_H
    if (check_open_lines())
        return 1;
#endif
    return 0;
}
