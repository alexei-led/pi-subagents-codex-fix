#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <libproc.h>
#include <signal.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/sysctl.h>
#include <time.h>
#include <unistd.h>
#include <uuid/uuid.h>

/* Private ABI, pinned to XNU f6217f891ac0bb64f3d375211650a4c1ff8ca1ea:
 * https://github.com/apple-oss-distributions/xnu/blob/f6217f891ac0bb64f3d375211650a4c1ff8ca1ea/bsd/sys/proc_info_private.h
 * https://github.com/apple-oss-distributions/xnu/blob/f6217f891ac0bb64f3d375211650a4c1ff8ca1ea/osfmk/mach/coalition.h
 * https://github.com/apple-oss-distributions/xnu/blob/f6217f891ac0bb64f3d375211650a4c1ff8ca1ea/bsd/kern/sys_coalition.c
 * The usage syscall copies MIN(requested size, kernel size), so request only
 * its two-counter prefix. These assertions check layout, not OS compatibility.
 */
struct unique_info {
    uint8_t executable_uuid[16];
    uint64_t unique_id, parent_unique_id;
    int32_t pid_version, parent_pid_version;
    uint64_t reserved[2];
};
struct coalition_info { uint64_t ids[2], reserved[3]; };
struct usage_prefix { uint64_t started, exited; };
struct identity { pid_t pid; struct unique_info unique; uint64_t coalition; };
_Static_assert(sizeof(struct unique_info) == 56, "unique ABI size");
_Static_assert(offsetof(struct unique_info, unique_id) == 16, "unique ABI offset");
_Static_assert(offsetof(struct unique_info, pid_version) == 32, "version ABI offset");
_Static_assert(sizeof(struct coalition_info) == 40, "coalition ABI size");
_Static_assert(offsetof(struct coalition_info, ids) == 0, "coalition ABI offset");
_Static_assert(sizeof(struct usage_prefix) == 16, "usage ABI size");
_Static_assert(offsetof(struct usage_prefix, exited) == 8, "usage ABI offset");
_Static_assert(sizeof(audit_token_t) == 32, "audit token ABI size");
extern int coalition_info_resource_usage(uint64_t, void *, size_t) __attribute__((weak_import));
extern int proc_signal_with_audittoken(audit_token_t *, int) __attribute__((weak_import));

static void json_string(const char *s) {
    putchar('"');
    for (const unsigned char *p = (const unsigned char *)s; *p; ++p) {
        if (*p == '"' || *p == '\\') printf("\\%c", *p);
        else if (*p < 32) printf("\\u%04x", *p);
        else putchar(*p);
    }
    putchar('"');
}

static int failure(int error, const char *detail) {
    printf("{\"ok\":false,\"errno\":%d,\"error\":", error ? error : EIO);
    json_string(detail);
    puts("}");
    return 1;
}

static bool number(const char *s, uint64_t max, uint64_t *value) {
    if (!s || !*s) return false;
    for (const char *p = s; *p; ++p) if (*p < '0' || *p > '9') return false;
    errno = 0;
    char *end;
    uintmax_t parsed = strtoumax(s, &end, 10);
    if (errno || *end || parsed == 0 || parsed > max) return false;
    *value = (uint64_t)parsed;
    return true;
}

static int read_info(pid_t pid, int flavor, void *buffer, int size) {
    errno = 0;
    int read = proc_pidinfo(pid, flavor, 0, buffer, size);
    return read == size ? 0 : (errno ? errno : EIO);
}

static int inspect(pid_t pid, struct identity *out) {
    for (int attempt = 0; attempt < 3; ++attempt) {
        struct unique_info before = {0}, after = {0};
        struct coalition_info coalition = {0};
        int error = read_info(pid, 17, &before, sizeof(before));
        if (error) return error;
        error = read_info(pid, 20, &coalition, sizeof(coalition));
        if (error) return error;
        error = read_info(pid, 17, &after, sizeof(after));
        if (error) return error;
        if (before.unique_id != after.unique_id || before.pid_version != after.pid_version) continue;
        if (!after.unique_id || !coalition.ids[0]) return EIO;
        *out = (struct identity){ .pid = pid, .unique = after, .coalition = coalition.ids[0] };
        return 0;
    }
    return EAGAIN;
}

static void print_identity(const struct identity *id) {
    printf("{\"pid\":%d,\"uniqueId\":\"%" PRIu64 "\",\"pidVersion\":%" PRIu32 "}",
        id->pid, id->unique.unique_id, (uint32_t)id->unique.pid_version);
}

static int host(void) {
    uuid_t host_uuid, boot_uuid;
    struct timespec timeout = { .tv_sec = 2, .tv_nsec = 0 };
    if (gethostuuid(host_uuid, &timeout)) return failure(errno, "host identity unavailable");
    char boot[37] = {0};
    size_t size = sizeof(boot);
    if (sysctlbyname("kern.bootsessionuuid", boot, &size, NULL, 0))
        return failure(errno, "boot identity unavailable");
    if (size != sizeof(boot) || boot[36] != '\0' || uuid_parse(boot, boot_uuid) ||
        uuid_is_null(host_uuid) || uuid_is_null(boot_uuid))
        return failure(EIO, "invalid host or boot identity");
    char host_text[37];
    uuid_unparse_lower(host_uuid, host_text);
    uuid_unparse_lower(boot_uuid, boot);
    /* System-wide, includes sleep, unaffected by wall-clock adjustments:
     * https://github.com/apple-oss-distributions/Libc/blob/main/gen/clock_gettime.3
     * Compare only within the same boot and against this clock source.
     */
    struct timespec monotonic;
    if (clock_gettime(CLOCK_MONOTONIC_RAW, &monotonic))
        return failure(errno, "monotonic clock unavailable");
    if (monotonic.tv_sec < 0 || monotonic.tv_nsec < 0 || monotonic.tv_nsec >= 1000000000L ||
        (uint64_t)monotonic.tv_sec > (UINT64_MAX - (uint64_t)monotonic.tv_nsec) / UINT64_C(1000000000))
        return failure(EOVERFLOW, "invalid monotonic clock value");
    uint64_t monotonic_ns = (uint64_t)monotonic.tv_sec * UINT64_C(1000000000) + (uint64_t)monotonic.tv_nsec;
    printf("{\"ok\":true,\"hostId\":\"%s\",\"bootId\":\"%s\",\"platform\":\"darwin\",\"abi\":1,\"monotonicNs\":\"%" PRIu64 "\"}\n",
        host_text, boot, monotonic_ns);
    return 0;
}

static int usage(uint64_t coalition) {
    if (!coalition_info_resource_usage) return failure(ENOTSUP, "coalition API unavailable");
    struct usage_prefix result = { .started = UINT64_MAX, .exited = UINT64_MAX };
    errno = 0;
    if (coalition_info_resource_usage(coalition, &result, sizeof(result)))
        return failure(errno, "coalition usage unavailable");
    if (result.started == UINT64_MAX || result.exited > result.started)
        return failure(EIO, "invalid coalition accounting");
    printf("{\"ok\":true,\"started\":\"%" PRIu64 "\",\"exited\":\"%" PRIu64 "\"}\n", result.started, result.exited);
    return 0;
}

static int members(uint64_t coalition) {
    errno = 0;
    int count = proc_listallpids(NULL, 0);
    if (count <= 0) return failure(errno, "process enumeration unavailable");
    if (count > INT_MAX / (int)sizeof(pid_t) - 1024) return failure(EOVERFLOW, "process list too large");
    int capacity = count + 1024;
    pid_t *pids = calloc((size_t)capacity, sizeof(*pids));
    if (!pids) return failure(ENOMEM, "process list allocation failed");
    errno = 0;
    count = proc_listallpids(pids, capacity * (int)sizeof(*pids));
    if (count <= 0) {
        int error = errno;
        free(pids);
        return failure(error, "process enumeration failed");
    }
    bool incomplete = count >= capacity, first = true;
    if (count > capacity) count = capacity;
    printf("{\"ok\":true,\"members\":[");
    for (int i = 0; i < count; ++i) {
        if (pids[i] <= 0 || pids[i] == getpid()) continue;
        struct identity id;
        if (inspect(pids[i], &id)) { incomplete = true; continue; }
        if (id.coalition != coalition) continue;
        if (!first) putchar(',');
        print_identity(&id);
        first = false;
    }
    free(pids);
    /* Even a successful scan races forks; it never proves terminal emptiness. */
    printf("],\"incomplete\":%s}\n", incomplete ? "true" : "false");
    return 0;
}

static int send_signal(pid_t pid, uint32_t version, uint64_t unique, uint64_t coalition, int sig) {
    if (pid == getpid() || pid == getppid() || pid <= 1)
        return failure(EPERM, "refusing to signal observer or supervisor");
    if (!proc_signal_with_audittoken) return failure(ENOTSUP, "audit-token signal API unavailable");
    struct identity id;
    int error = inspect(pid, &id);
    if (error) return failure(error, "signal identity unavailable");
    if (id.unique.unique_id != unique || (uint32_t)id.unique.pid_version != version)
        return failure(ESRCH, "process generation mismatch");
    if (id.coalition != coalition) return failure(EPERM, "process coalition mismatch");
    /* The kernel atomically verifies token PID/version, including exec races.
     * Resource coalition membership is immutable during that task generation.
     * https://github.com/apple-oss-distributions/xnu/blob/f6217f891ac0bb64f3d375211650a4c1ff8ca1ea/bsd/kern/proc_info.c
     */
    audit_token_t token = {{0, geteuid(), getegid(), getuid(), getgid(), (uint32_t)pid, 0, version}};
    errno = 0;
    int result = proc_signal_with_audittoken(&token, sig);
    if (result != 0) return failure(result > 0 ? result : errno, "audit-token signal failed");
    puts("{\"ok\":true}");
    return 0;
}

int main(int argc, char **argv) {
    if (argc == 2 && !strcmp(argv[1], "host")) return host();
    uint64_t value;
    if (argc == 3 && !strcmp(argv[1], "inspect") && number(argv[2], INT_MAX, &value)) {
        struct identity id;
        int error = inspect((pid_t)value, &id);
        if (error) return failure(error, "process identity unavailable");
        printf("{\"ok\":true,\"identity\":");
        print_identity(&id);
        printf(",\"coalitionId\":\"%" PRIu64 "\"}\n", id.coalition);
        return 0;
    }
    if (argc == 3 && number(argv[2], UINT64_MAX, &value)) {
        if (!strcmp(argv[1], "coalition")) return usage(value);
        if (!strcmp(argv[1], "members")) return members(value);
    }
    uint64_t pid, version, unique, coalition, sig;
    if (argc == 7 && !strcmp(argv[1], "signal") && number(argv[2], INT_MAX, &pid) &&
        number(argv[3], UINT32_MAX, &version) && number(argv[4], UINT64_MAX, &unique) &&
        number(argv[5], UINT64_MAX, &coalition) && number(argv[6], NSIG - 1, &sig))
        return send_signal((pid_t)pid, (uint32_t)version, unique, coalition, (int)sig);
    return failure(EINVAL, "invalid command or arguments");
}
