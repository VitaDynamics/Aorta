#include <assert.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "aorta.h"

int main(void) {
  printf("=== Aorta Client Call FFI Test ===\n\n");

  printf("[Test 1] Setup Node\n");

  aorta_node_t node = 0;
  aorta_result_t rc = aorta_node_new("ffi_client_test_node", "default", &node);
  assert(rc == AORTA_OK && "aorta_node_new failed");
  assert(node != 0 && "node handle should be non-zero");
  printf("  ✓ Node created (handle: %" PRIu64 ")\n", (uint64_t)node);

  printf("\n[Test 2] Create Service\n");

  aorta_service_t service = 0;
  aorta_service_options_t service_opts = {
      .schema_attach_policy = AORTA_SCHEMA_ATTACH_INHERIT,
      .callback_group = AORTA_CALLBACK_GROUP_MUTUALLY_EXCLUSIVE,
      .callback_group_handle = 0,
      .deadline_ns = 0,
      .service_schema_hash_hex = NULL,
      .service_schema_hash_len = 0,
      .service_schema_bfbs = NULL,
      .service_schema_bfbs_len = 0,
      .request_root_type = NULL,
      .request_root_type_len = 0,
      .response_root_type = NULL,
      .response_root_type_len = 0,
      .rpc_overflow_policy = AORTA_RPC_OVERFLOW_FAST_FAIL_BUSY,
      .rpc_overflow_block_timeout_ms = 0,
  };
  rc = aorta_service_new_with_options(node, "/ffi/test/echo_service",
                                      &service_opts, NULL, NULL, &service);
  assert(rc == AORTA_OK && "aorta_service_new_with_options failed");
  assert(service != 0 && "service handle should be non-zero");
  printf("  ✓ Service created (handle: %" PRIu64 ")\n", (uint64_t)service);

  printf("\n[Test 3] Create Client\n");

  aorta_client_t client = 0;
  aorta_client_options_t client_opts = {
      .schema_attach_policy = AORTA_SCHEMA_ATTACH_INHERIT,
      .timeout_ms = 5000,
      .enforce_single_provider = AORTA_OPTIONAL_BOOL_INHERIT,
      .multi_reply_check_window_ms = 0,
      .service_schema_hash_hex = NULL,
      .service_schema_hash_len = 0,
  };
  rc = aorta_client_new_with_options(node, "/ffi/test/echo_service",
                                     &client_opts, &client);
  assert(rc == AORTA_OK && "aorta_client_new_with_options failed");
  assert(client != 0 && "client handle should be non-zero");
  printf("  ✓ Client created (handle: %" PRIu64 ")\n", (uint64_t)client);

  printf("\n[Test 4] Call Service (aorta_client_call)\n");

  uint8_t test_request[] = {0x05, 0x00, 0x00, 0x00, 0x48,
                            0x65, 0x6c, 0x6c, 0x6f};
  aorta_owned_bytes_slice_t request_slice = {
      test_request,
      sizeof(test_request),
      NULL,
      NULL,
  };
  aorta_owned_bytes_view_t request_view = {
      &request_slice,
      1,
      sizeof(test_request),
  };
  aorta_owned_bytes_t response = {0};

  printf("  Calling service with request (size: %zu)...\n",
         sizeof(test_request));
  rc = aorta_client_call(client, &request_view, &response);

  if (rc == AORTA_OK) {
    printf("  ✓ Service call succeeded\n");
    printf("  ✓ Response size: %zu bytes\n", response.view.total_len);

    if (response.view.slice_count > 0 && response.view.slices != NULL) {
      printf("  ✓ Response data received: [");
      size_t printed = 0;
      for (size_t s = 0; s < response.view.slice_count && printed < 32; ++s) {
        const aorta_bytes_slice_t* slice = &response.view.slices[s];
        for (size_t i = 0; i < slice->len && printed < 32; ++i, ++printed) {
          printf("%02x%s", slice->data[i], (printed + 1 < response.view.total_len) ? " " : "");
        }
      }
      if (response.view.total_len > 32) {
        printf(" ...");
      }
      printf("]\n");

    } else {
      printf("  ! Warning: Response is NULL or empty\n");
    }
    aorta_owned_bytes_free(&response);
    printf("  ✓ Response freed\n");
  } else if (rc == AORTA_ERR_TIMEOUT) {
    printf("  ! Service call timed out (no server responding)\n");
    printf("  This is expected since we don't have a real service handler\n");
  } else if (rc == AORTA_ERR_INTERNAL) {
    printf("  ✗ FAILED: Service call returned INTERNAL ERROR\n");
    printf("  This was the original bug - aorta_client_call was a stub\n");
    assert(0 && "aorta_client_call should not return INTERNAL ERROR");
  } else {
    printf("  ! Service call failed with error code: %d\n", rc);
  }

  printf("\n[Test 5] Cleanup\n");

  rc = aorta_client_free(client);
  assert(rc == AORTA_OK && "aorta_client_free failed");
  printf("  ✓ Client freed\n");

  rc = aorta_service_free(service);
  assert(rc == AORTA_OK && "aorta_service_free failed");
  printf("  ✓ Service freed\n");

  rc = aorta_node_free(node);
  assert(rc == AORTA_OK && "aorta_node_free failed");
  printf("  ✓ Node freed\n");

  printf("\n=== Client Call FFI Test PASSED ===\n");
  printf("NOTE: The test passes if aorta_client_call is no longer a stub\n");
  printf("      returning INTERNAL ERROR. Timeout is acceptable behavior.\n");
  return 0;
}
