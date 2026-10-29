#ifndef AORTA_INTROSPECT_FB_REFLECT_H_
#define AORTA_INTROSPECT_FB_REFLECT_H_

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

int aorta_introspect_fb_to_json(const uint8_t* bfbs_data,
                                size_t bfbs_size,
                                const uint8_t* payload_data,
                                size_t payload_size,
                                const char* root_type,
                                size_t root_type_size,
                                char** out_json);

int aorta_introspect_fb_to_json_display(const uint8_t* bfbs_data,
                                        size_t bfbs_size,
                                        const uint8_t* payload_data,
                                        size_t payload_size,
                                        const char* root_type,
                                        size_t root_type_size,
                                        char** out_json);

int aorta_introspect_bfbs_to_schema_text(const uint8_t* bfbs_data,
                                         size_t bfbs_size,
                                         const char* root_type,
                                         size_t root_type_size,
                                         char** out_text);

void aorta_introspect_free_string(char* str);

const char* aorta_introspect_last_error(void);

#ifdef __cplusplus
}
#endif

#endif  // AORTA_INTROSPECT_FB_REFLECT_H_
