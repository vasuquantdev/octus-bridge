#include <stddef.h>

static inline void wrpkru(unsigned int pkru) {
    unsigned int eax = pkru;
    unsigned int ecx = 0;
    unsigned int edx = 0;

    asm volatile(
    ".byte 0x0f,0x01,0xef\n\t"
    : // empty output
    : "a" (eax), "c" (ecx), "d" (edx)
    );
}

int pkey_set(int pkey, size_t rights) {
    unsigned int pkru = (rights << (2 * pkey));
    wrpkru(pkru);
    return 0;
}
