#ifndef AORTA_INTROSPECT_JSON_TO_FB_H_
#define AORTA_INTROSPECT_JSON_TO_FB_H_

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

int aorta_introspect_json_to_fb(const uint8_t* bfbs_data,
                                size_t bfbs_size,
                                const char* json_data,
                                size_t json_size,
                                const char* root_type,
                                size_t root_type_size,
                                uint8_t** out_fb,
                                size_t* out_fb_size);

void aorta_introspect_free_bytes(uint8_t* data);

const char* aorta_introspect_json_to_fb_last_error(void);

#ifdef __cplusplus
}
#endif

#endif  // AORTA_INTROSPECT_JSON_TO_FB_H_
