/*
 * Custom Memory Allocator Implementation
 * Features: Heap management, fragmentation control, benchmarking vs libc
 * Compile: gcc -O2 -o custom_allocator custom_allocator.c
 * Run: ./custom_allocator
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>
#include <stdint.h>
#include <time.h>
#include <assert.h>

/* ============================================================================
 * CONFIGURATION
 * ============================================================================ */

#define HEAP_SIZE (1024 * 1024 * 100)  /* 100MB heap */
#define MIN_BLOCK_SIZE 32              /* Minimum allocation size */
#define ALIGNMENT 16                   /* Memory alignment */
#define ALIGN(size) (((size) + (ALIGNMENT-1)) & ~(ALIGNMENT-1))

/* ============================================================================
 * DATA STRUCTURES
 * ============================================================================ */

typedef struct BlockHeader {
    size_t size;              /* Size of the block (including header) */
    int    free;              /* 1 if free, 0 if allocated */
    struct BlockHeader *next; /* Next block in list */
    struct BlockHeader *prev; /* Previous block in list */
    uint32_t magic;           /* Magic number for corruption detection */
} BlockHeader;

#define BLOCK_HEADER_SIZE ALIGN(sizeof(BlockHeader))
#define MAGIC_NUMBER 0xDEADBEEF

/* Heap structure */
typedef struct {
    void *heap_start;         /* Start of heap memory */
    void *heap_end;           /* End of heap memory */
    size_t total_size;      /* Total heap size */
    size_t used_bytes;      /* Currently allocated bytes */
    size_t free_bytes;      /* Currently free bytes */
    BlockHeader *free_list; /* Head of free list */
    int init;               /* Initialization flag */
} Heap;

/* Global heap instance */
static Heap g_heap = {0};

/* Statistics for benchmarking */
typedef struct {
    size_t allocations;
    size_t deallocations;
    size_t total_allocated;
    size_t total_freed;
    size_t peak_usage;
    size_t current_usage;
    size_t fragmentation_events;
    size_t coalesce_count;
} AllocStats;

static AllocStats g_stats = {0};

/* ============================================================================
 * HEAP INITIALIZATION
 * ============================================================================ */

int heap_init(void) {
    if (g_heap.init) return 0;

    /* Allocate heap using mmap */
    g_heap.heap_start = mmap(NULL, HEAP_SIZE, PROT_READ | PROT_WRITE,
                              MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (g_heap.heap_start == MAP_FAILED) {
        perror("mmap failed");
        return -1;
    }

    g_heap.heap_end = (char *)g_heap.heap_start + HEAP_SIZE;
    g_heap.total_size = HEAP_SIZE;
    g_heap.used_bytes = 0;
    g_heap.free_bytes = HEAP_SIZE - BLOCK_HEADER_SIZE;

    /* Initialize first block */
    BlockHeader *initial = (BlockHeader *)g_heap.heap_start;
    initial->size = HEAP_SIZE;
    initial->free = 1;
    initial->next = NULL;
    initial->prev = NULL;
    initial->magic = MAGIC_NUMBER;

    g_heap.free_list = initial;
    g_heap.init = 1;

    printf("[Heap] Initialized %zu MB heap at %p\n", HEAP_SIZE / (1024*1024), g_heap.heap_start);
    return 0;
}

void heap_destroy(void) {
    if (!g_heap.init) return;
    munmap(g_heap.heap_start, g_heap.total_size);
    memset(&g_heap, 0, sizeof(g_heap));
    memset(&g_stats, 0, sizeof(g_stats));
    printf("[Heap] Destroyed\n");
}

/* ============================================================================
 * BLOCK OPERATIONS
 * ============================================================================ */

/* Split a free block if it's large enough */
static void split_block(BlockHeader *block, size_t size) {
    size_t remaining = block->size - size;

    if (remaining >= BLOCK_HEADER_SIZE + MIN_BLOCK_SIZE) {
        BlockHeader *new_block = (BlockHeader *)((char *)block + size);
        new_block->size = remaining;
        new_block->free = 1;
        new_block->magic = MAGIC_NUMBER;
        new_block->next = block->next;
        new_block->prev = block;

        if (block->next) block->next->prev = new_block;
        block->next = new_block;
        block->size = size;

        g_stats.fragmentation_events++;
    }
}

/* Coalesce adjacent free blocks */
static BlockHeader *coalesce(BlockHeader *block) {
    if (!block) return NULL;

    int coalesced = 0;

    /* Coalesce with next block */
    if (block->next && block->next->free) {
        block->size += block->next->size;
        block->next = block->next->next;
        if (block->next) block->next->prev = block;
        coalesced = 1;
    }

    /* Coalesce with previous block */
    if (block->prev && block->prev->free) {
        block->prev->size += block->size;
        block->prev->next = block->next;
        if (block->next) block->next->prev = block->prev;
        block = block->prev;
        coalesced = 1;
    }

    if (coalesced) g_stats.coalesce_count++;

    return block;
}

/* Find best-fit free block */
static BlockHeader *find_best_fit(size_t size) {
    BlockHeader *current = g_heap.free_list;
    BlockHeader *best = NULL;
    size_t best_size = SIZE_MAX;

    while (current) {
        if (current->free && current->size >= size) {
            if (current->size < best_size) {
                best = current;
                best_size = current->size;
                if (best_size == size) break; /* Exact match */
            }
        }
        current = current->next;
    }

    return best;
}

/* Find first-fit free block (faster, more fragmentation) */
static BlockHeader *find_first_fit(size_t size) {
    BlockHeader *current = g_heap.free_list;

    while (current) {
        if (current->free && current->size >= size) {
            return current;
        }
        current = current->next;
    }

    return NULL;
}

/* ============================================================================
 * CUSTOM MALLOC/FREE
 * ============================================================================ */

void *custom_malloc(size_t size) {
    if (!g_heap.init && heap_init() != 0) return NULL;
    if (size == 0) return NULL;

    size_t aligned_size = ALIGN(size);
    size_t total_size = BLOCK_HEADER_SIZE + aligned_size;

    if (total_size < MIN_BLOCK_SIZE) total_size = MIN_BLOCK_SIZE;

    /* Find suitable block (using best-fit for less fragmentation) */
    BlockHeader *block = find_best_fit(total_size);

    if (!block) {
        fprintf(stderr, "[Allocator] Out of memory! Requested: %zu bytes\n", size);
        return NULL;
    }

    /* Split if too large */
    split_block(block, total_size);

    /* Mark as allocated */
    block->free = 0;
    block->magic = MAGIC_NUMBER;

    /* Update stats */
    g_stats.allocations++;
    g_stats.total_allocated += aligned_size;
    g_stats.current_usage += aligned_size;
    g_heap.used_bytes += aligned_size;
    g_heap.free_bytes -= total_size;

    if (g_stats.current_usage > g_stats.peak_usage) {
        g_stats.peak_usage = g_stats.current_usage;
    }

    void *ptr = (char *)block + BLOCK_HEADER_SIZE;
    memset(ptr, 0, aligned_size); /* Zero memory for safety */

    return ptr;
}

void custom_free(void *ptr) {
    if (!ptr) return;

    BlockHeader *block = (BlockHeader *)((char *)ptr - BLOCK_HEADER_SIZE);

    /* Safety checks */
    if (block->magic != MAGIC_NUMBER) {
        fprintf(stderr, "[Allocator] Corruption detected at %p!\n", ptr);
        return;
    }

    if (block->free) {
        fprintf(stderr, "[Allocator] Double free detected at %p!\n", ptr);
        return;
    }

    size_t data_size = block->size - BLOCK_HEADER_SIZE;

    /* Mark as free */
    block->free = 1;
    block->magic = MAGIC_NUMBER;

    /* Coalesce with neighbors */
    block = coalesce(block);

    /* Update stats */
    g_stats.deallocations++;
    g_stats.total_freed += data_size;
    g_stats.current_usage -= data_size;
    g_heap.used_bytes -= data_size;
    g_heap.free_bytes += block->size;
}

void *custom_realloc(void *ptr, size_t size) {
    if (!ptr) return custom_malloc(size);
    if (size == 0) {
        custom_free(ptr);
        return NULL;
    }

    BlockHeader *block = (BlockHeader *)((char *)ptr - BLOCK_HEADER_SIZE);
    size_t old_size = block->size - BLOCK_HEADER_SIZE;

    void *new_ptr = custom_malloc(size);
    if (new_ptr) {
        size_t copy_size = (size < old_size) ? size : old_size;
        memcpy(new_ptr, ptr, copy_size);
        custom_free(ptr);
    }

    return new_ptr;
}

void *custom_calloc(size_t nmemb, size_t size) {
    size_t total = nmemb * size;
    void *ptr = custom_malloc(total);
    if (ptr) memset(ptr, 0, total);
    return ptr;
}

/* ============================================================================
 * DIAGNOSTICS
 * ============================================================================ */

void heap_dump(void) {
    printf("\n========== HEAP DUMP ==========\n");
    printf("Total Size:     %zu bytes (%.2f MB)\n",
           g_heap.total_size, g_heap.total_size / (1024.0 * 1024.0));
    printf("Used:           %zu bytes (%.2f MB)\n",
           g_heap.used_bytes, g_heap.used_bytes / (1024.0 * 1024.0));
    printf("Free:           %zu bytes (%.2f MB)\n",
           g_heap.free_bytes, g_heap.free_bytes / (1024.0 * 1024.0));
    printf("Utilization:    %.2f%%\n",
           (g_heap.used_bytes * 100.0) / g_heap.total_size);

    printf("\n--- Block List ---\n");
    BlockHeader *current = g_heap.free_list;
    int count = 0;
    size_t free_blocks = 0, allocated_blocks = 0;

    while (current) {
        printf("Block %d: addr=%p, size=%zu, %s\n",
               count, current, current->size,
               current->free ? "FREE" : "USED");

        if (current->free) free_blocks++;
        else allocated_blocks++;

        current = current->next;
        count++;
    }

    printf("\nTotal blocks: %d (Free: %zu, Allocated: %zu)\n",
           count, free_blocks, allocated_blocks);
    printf("Fragmentation events: %zu\n", g_stats.fragmentation_events);
    printf("Coalesce operations:  %zu\n", g_stats.coalesce_count);
    printf("================================\n\n");
}

void print_stats(void) {
    printf("\n========== STATISTICS ==========\n");
    printf("Allocations:        %zu\n", g_stats.allocations);
    printf("Deallocations:      %zu\n", g_stats.deallocations);
    printf("Total allocated:    %zu bytes\n", g_stats.total_allocated);
    printf("Total freed:        %zu bytes\n", g_stats.total_freed);
    printf("Peak usage:         %zu bytes (%.2f MB)\n",
           g_stats.peak_usage, g_stats.peak_usage / (1024.0 * 1024.0));
    printf("Current usage:      %zu bytes\n", g_stats.current_usage);
    printf("Fragmentation:      %zu events\n", g_stats.fragmentation_events);
    printf("Coalesces:          %zu\n", g_stats.coalesce_count);
    printf("================================\n\n");
}

/* ============================================================================
 * BENCHMARKING
 * ============================================================================ */

typedef struct {
    double custom_time;
    double libc_time;
    size_t iterations;
    const char *test_name;
} BenchmarkResult;

static double get_time(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

/* Test 1: Small allocations */
BenchmarkResult bench_small_allocs(size_t iterations) {
    BenchmarkResult res = {0};
    res.iterations = iterations;
    res.test_name = "Small allocations (64 bytes)";

    /* Custom allocator */
    heap_destroy();
    heap_init();

    double start = get_time();
    void **ptrs = malloc(iterations * sizeof(void *));

    for (size_t i = 0; i < iterations; i++) {
        ptrs[i] = custom_malloc(64);
    }
    for (size_t i = 0; i < iterations; i++) {
        custom_free(ptrs[i]);
    }

    res.custom_time = get_time() - start;
    free(ptrs);

    /* libc allocator */
    ptrs = malloc(iterations * sizeof(void *));
    start = get_time();

    for (size_t i = 0; i < iterations; i++) {
        ptrs[i] = malloc(64);
    }
    for (size_t i = 0; i < iterations; i++) {
        free(ptrs[i]);
    }

    res.libc_time = get_time() - start;
    free(ptrs);

    return res;
}

/* Test 2: Mixed size allocations */
BenchmarkResult bench_mixed_allocs(size_t iterations) {
    BenchmarkResult res = {0};
    res.iterations = iterations;
    res.test_name = "Mixed size allocations";

    size_t sizes[] = {16, 64, 256, 1024, 4096, 16384};
    size_t num_sizes = sizeof(sizes) / sizeof(sizes[0]);

    /* Custom allocator */
    heap_destroy();
    heap_init();

    double start = get_time();
    void **ptrs = malloc(iterations * sizeof(void *));

    for (size_t i = 0; i < iterations; i++) {
        ptrs[i] = custom_malloc(sizes[i % num_sizes]);
    }
    for (size_t i = 0; i < iterations; i++) {
        custom_free(ptrs[i]);
    }

    res.custom_time = get_time() - start;
    free(ptrs);

    /* libc allocator */
    ptrs = malloc(iterations * sizeof(void *));
    start = get_time();

    for (size_t i = 0; i < iterations; i++) {
        ptrs[i] = malloc(sizes[i % num_sizes]);
    }
    for (size_t i = 0; i < iterations; i++) {
        free(ptrs[i]);
    }

    res.libc_time = get_time() - start;
    free(ptrs);

    return res;
}

/* Test 3: Allocate-then-free pattern (fragmentation test) */
BenchmarkResult bench_fragmentation(size_t iterations) {
    BenchmarkResult res = {0};
    res.iterations = iterations;
    res.test_name = "Fragmentation pattern (alternating sizes)";

    /* Custom allocator */
    heap_destroy();
    heap_init();

    double start = get_time();
    void **ptrs = calloc(iterations, sizeof(void *));

    /* Allocate alternating small and large blocks */
    for (size_t i = 0; i < iterations; i++) {
        ptrs[i] = custom_malloc((i % 2 == 0) ? 64 : 1024);
    }
    /* Free every other block */
    for (size_t i = 0; i < iterations; i += 2) {
        custom_free(ptrs[i]);
    }
    /* Allocate medium blocks to test fragmentation handling */
    for (size_t i = 0; i < iterations; i += 2) {
        ptrs[i] = custom_malloc(512);
    }
    /* Free all */
    for (size_t i = 0; i < iterations; i++) {
        custom_free(ptrs[i]);
    }

    res.custom_time = get_time() - start;
    free(ptrs);

    /* libc allocator */
    ptrs = calloc(iterations, sizeof(void *));
    start = get_time();

    for (size_t i = 0; i < iterations; i++) {
        ptrs[i] = malloc((i % 2 == 0) ? 64 : 1024);
    }
    for (size_t i = 0; i < iterations; i += 2) {
        free(ptrs[i]);
    }
    for (size_t i = 0; i < iterations; i += 2) {
        ptrs[i] = malloc(512);
    }
    for (size_t i = 0; i < iterations; i++) {
        free(ptrs[i]);
    }

    res.libc_time = get_time() - start;
    free(ptrs);

    return res;
}

/* Test 4: Realloc pattern */
BenchmarkResult bench_realloc(size_t iterations) {
    BenchmarkResult res = {0};
    res.iterations = iterations;
    res.test_name = "Realloc pattern (growing buffers)";

    /* Custom allocator */
    heap_destroy();
    heap_init();

    double start = get_time();

    for (size_t i = 0; i < iterations; i++) {
        void *ptr = custom_malloc(64);
        ptr = custom_realloc(ptr, 128);
        ptr = custom_realloc(ptr, 256);
        ptr = custom_realloc(ptr, 512);
        custom_free(ptr);
    }

    res.custom_time = get_time() - start;

    /* libc allocator */
    start = get_time();

    for (size_t i = 0; i < iterations; i++) {
        void *ptr = malloc(64);
        ptr = realloc(ptr, 128);
        ptr = realloc(ptr, 256);
        ptr = realloc(ptr, 512);
        free(ptr);
    }

    res.libc_time = get_time() - start;

    return res;
}

void print_benchmark(BenchmarkResult *res) {
    printf("\n%-40s\n", res->test_name);
    printf("  Iterations:  %zu\n", res->iterations);
    printf("  Custom:      %.4f sec\n", res->custom_time);
    printf("  libc:        %.4f sec\n", res->libc_time);
    printf("  Ratio:       %.2fx %s\n",
           res->custom_time / res->libc_time,
           res->custom_time < res->libc_time ? "(custom faster)" : "(libc faster)");
    printf("  Throughput:  %.0f ops/sec (custom), %.0f ops/sec (libc)\n",
           res->iterations / res->custom_time,
           res->iterations / res->libc_time);
}

/* ============================================================================
 * STRESS TEST
 * ============================================================================ */

void stress_test(void) {
    printf("\n========== STRESS TEST ==========\n");

    heap_destroy();
    heap_init();

    #define STRESS_ITERATIONS 10000
    #define MAX_PTRS 1000

    void *ptrs[MAX_PTRS] = {0};
    size_t sizes[MAX_PTRS] = {0};

    srand(42); /* Deterministic seed */

    for (int round = 0; round < 10; round++) {
        printf("Round %d/10...\n", round + 1);

        /* Random allocations */
        for (int i = 0; i < STRESS_ITERATIONS; i++) {
            int idx = rand() % MAX_PTRS;

            if (ptrs[idx]) {
                custom_free(ptrs[idx]);
                ptrs[idx] = NULL;
            }

            sizes[idx] = (rand() % 8192) + 16;
            ptrs[idx] = custom_malloc(sizes[idx]);

            if (ptrs[idx]) {
                /* Write pattern to detect corruption */
                memset(ptrs[idx], idx & 0xFF, sizes[idx]);
            }
        }

        /* Verify all allocated memory */
        for (int i = 0; i < MAX_PTRS; i++) {
            if (ptrs[i]) {
                unsigned char *p = ptrs[i];
                for (size_t j = 0; j < sizes[i]; j++) {
                    if (p[j] != (i & 0xFF)) {
                        fprintf(stderr, "Memory corruption detected at ptr[%d]!\n", i);
                        abort();
                    }
                }
            }
        }
    }

    /* Cleanup */
    for (int i = 0; i < MAX_PTRS; i++) {
        if (ptrs[i]) custom_free(ptrs[i]);
    }

    printf("Stress test PASSED!\n");
    print_stats();
}

/* ============================================================================
 * MAIN
 * ============================================================================ */

int main(int argc, char *argv[]) {
    printf("========================================\n");
    printf("  Custom Memory Allocator Benchmark\n");
    printf("========================================\n");
    printf("Heap size: %d MB\n", HEAP_SIZE / (1024 * 1024));
    printf("Alignment: %d bytes\n", ALIGNMENT);
    printf("Min block: %d bytes\n\n", MIN_BLOCK_SIZE);

    /* Run stress test first */
    stress_test();

    /* Run benchmarks */
    printf("\n========== BENCHMARKS ==========\n");

    BenchmarkResult res1 = bench_small_allocs(100000);
    print_benchmark(&res1);

    BenchmarkResult res2 = bench_mixed_allocs(50000);
    print_benchmark(&res2);

    BenchmarkResult res3 = bench_fragmentation(10000);
    print_benchmark(&res3);

    BenchmarkResult res4 = bench_realloc(10000);
    print_benchmark(&res4);

    /* Final summary */
    printf("\n========== SUMMARY ==========\n");
    printf("Custom allocator features:\n");
    printf("  - Best-fit allocation strategy\n");
    printf("  - Block splitting for efficiency\n");
    printf("  - Automatic coalescing on free\n");
    printf("  - Memory alignment (%d bytes)\n", ALIGNMENT);
    printf("  - Corruption detection (magic numbers)\n");
    printf("  - Double-free protection\n");
    printf("  - Fragmentation statistics\n\n");

    double total_custom = res1.custom_time + res2.custom_time +
                          res3.custom_time + res4.custom_time;
    double total_libc = res1.libc_time + res2.libc_time +
                        res3.libc_time + res4.libc_time;

    printf("Total time - Custom: %.3f sec, libc: %.3f sec\n",
           total_custom, total_libc);
    printf("Overall ratio: %.2fx %s\n\n",
           total_custom / total_libc,
           total_custom < total_libc ? "(custom faster!)" : "(libc faster)");

    heap_destroy();
    return 0;
}
