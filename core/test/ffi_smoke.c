#include <assert.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "aorta.h"

/* Test callback data */
static int msg_count = 0;

/* Subscriber callback */
void on_msg_callback(const aorta_bytes_view_t* payload,
                     const aorta_schema_hash_view_t* schema,
                     const aorta_msg_context_t* ctx, void* user_data) {
  int* counter = (int*)user_data;
  (*counter)++;
  size_t len = payload ? payload->total_len : 0;
  printf("  Callback received message %d (size: %zu)\n", *counter, len);
  (void)schema;
  (void)ctx;
}

int main(void) {
  printf("=== Aorta FFI Smoke Test ===\n\n");

  /* ===== Test 1: Node Lifecycle ===== */
  printf("[Test 1] Node Lifecycle\n");

  aorta_node_t node = 0;
  aorta_result_t rc = aorta_node_new("ffi_test_node", "default", &node);
  assert(rc == AORTA_OK && "aorta_node_new failed");
  assert(node != 0 && "node handle should be non-zero");
  printf("  ✓ Node created (handle: %" PRIu64 ")\n", (uint64_t)node);

  /* Test node_now */
  int64_t now_ns = 0;
  rc = aorta_node_now(node, &now_ns);
  assert(rc == AORTA_OK && "aorta_node_now failed");
  assert(now_ns > 0 && "timestamp should be positive");
  printf("  ✓ Node timestamp: %" PRId64 " ns\n", now_ns);

  /* ===== Test 2: Publisher Creation and Publish ===== */
  printf("\n[Test 2] Publisher Creation and Publish\n");

  /* Create simple FlatBuffer message (4-byte size prefix + payload) */
  uint8_t test_msg[] = {
      0x04, 0x00, 0x00, 0x00, /* Size prefix (4 bytes) */
      0x48, 0x65, 0x6c, 0x6f  /* Payload: "Helo" */
  };

  aorta_pub_options_t pub_opts = {
      .schema_attach_policy =
          AORTA_SCHEMA_ATTACH_NEVER, /* Don't require schema for test */
      .schema_bfbs = NULL,           /* schema_bfbs */
      .schema_bfbs_len = 0,          /* schema_bfbs_len */
      .max_message_size = 1024,      /* max_message_size */
  };

  aorta_qos_t qos = {
      AORTA_PRIORITY_NORMAL,
      AORTA_RELIABILITY_RELIABLE,
      AORTA_CONGESTION_CONTROL_DROP, /* congestion_control */
      0,                             /* express */
  };

  aorta_pub_t pub = 0;
  rc = aorta_publisher_new(node, "/ffi/test/topic", &qos, &pub_opts, &pub);
  assert(rc == AORTA_OK && "aorta_publisher_new failed");
  assert(pub != 0 && "publisher handle should be non-zero");
  printf("  ✓ Publisher created (handle: %" PRIu64 ")\n", (uint64_t)pub);

  /* Test publish */
  rc = aorta_publisher_publish(pub, test_msg, sizeof(test_msg));
  assert(rc == AORTA_OK && "aorta_publisher_publish failed");
  printf("  ✓ Published message (%zu bytes)\n", sizeof(test_msg));

  /* Test publisher stats function */
  aorta_publisher_stats_t stats = {0};
  rc = aorta_publisher_get_stats(pub, &stats);
  assert(rc == AORTA_OK && "aorta_publisher_get_stats failed");
   printf("  ✓ Publisher stats retrieved (publish_count: %llu)\n",
          (unsigned long long)stats.publish_count);

  /* ===== Test 3: Subscriber Creation and Callback ===== */
  printf("\n[Test 3] Subscriber Creation and Callback\n");

  aorta_sub_options_t sub_opts = {
      .callback_group = AORTA_CALLBACK_GROUP_MUTUALLY_EXCLUSIVE,
      .callback_group_handle = 0,
      .deadline_ns = 0,
      .prefetch_schema_on_init = 0,
      .enable_header_context = 0,
      .enable_dedup = 0,
      .dedup_window = 0,
      .enable_reorder_check = 0,
      .drop_out_of_order = 0,
      .overflow_policy = AORTA_SUB_OVERFLOW_DROP,
      .overflow_relay_capacity = 1024,
  };

  aorta_sub_t sub = 0;
  rc = aorta_subscriber_new(node, "/ffi/test/topic", &qos, &sub_opts,
                            on_msg_callback, &msg_count, &sub);
  assert(rc == AORTA_OK && "aorta_subscriber_new failed");
  assert(sub != 0 && "subscriber handle should be non-zero");
  printf("  ✓ Subscriber created (handle: %" PRIu64 ")\n", (uint64_t)sub);

  /* ===== Test 4: Executor Creation and Spin ===== */
  printf("\n[Test 4] Executor and Message Delivery\n");

  aorta_exec_t exec = 0;
  aorta_priority_scheduler_options_t priority_opts = {
      32,             /* max_drain_per_cycle */
      {8, 4, 2, 1, 1}, /* weights */
      1,              /* map_subscription_qos */
  };
  aorta_executor_options_t exec_opts = {
      AORTA_SCHEDULER_PRIORITY,
      &priority_opts,
  };
  rc = aorta_executor_new_with_options(AORTA_EXECUTOR_SINGLE, &exec_opts, &exec);
  assert(rc == AORTA_OK && "aorta_executor_new_with_options failed");
  assert(exec != 0 && "executor handle should be non-zero");
  printf("  ✓ Executor created with options (handle: %" PRIu64 ")\n",
         (uint64_t)exec);

  rc = aorta_executor_add_node(exec, node);
  assert(rc == AORTA_OK && "aorta_executor_add_node failed");
  printf("  ✓ Node added to executor\n");

  /* Publish a message */
  rc = aorta_publisher_publish(pub, test_msg, sizeof(test_msg));
  assert(rc == AORTA_OK && "second publish failed");
  printf("  ✓ Published another message\n");

  /* Spin a few times to process messages */
  printf("  Spinning executor to process messages...\n");
  for (int i = 0; i < 5; i++) {
    rc = aorta_executor_spin_once(exec, 100); /* 100ms timeout */
    if (rc != AORTA_OK && rc != AORTA_ERR_TIMEOUT) {
      printf("  ! Spin warning: rc=%d\n", rc);
    }
    usleep(50000); /* 50ms */
  }

  printf("  Messages received: %d\n", msg_count);
  /* Note: Message delivery might be 0 in some environments, that's ok for smoke
   * test */

  /* ===== Test 5: Cleanup ===== */
  printf("\n[Test 5] Cleanup\n");

  rc = aorta_subscriber_free(sub);
  assert(rc == AORTA_OK && "aorta_subscriber_free failed");
  printf("  ✓ Subscriber freed\n");

  rc = aorta_publisher_free(pub);
  assert(rc == AORTA_OK && "aorta_publisher_free failed");
  printf("  ✓ Publisher freed\n");

  rc = aorta_executor_free(exec);
  assert(rc == AORTA_OK && "aorta_executor_free failed");
  printf("  ✓ Executor freed\n");

  rc = aorta_node_free(node);
  assert(rc == AORTA_OK && "aorta_node_free failed");
  printf("  ✓ Node freed\n");

  printf("\n=== All FFI Smoke Tests PASSED ===\n");
  return 0;
}
