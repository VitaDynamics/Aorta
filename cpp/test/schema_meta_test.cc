#include "aorta/schema_meta.h"
#include "schemas/topic/example/demo_chat_schema_meta.h"

#include <cassert>

int main() {
  using aorta::SchemaMeta;
  auto size = SchemaMeta<demo::Chat>::SchemaBfbsSize();
  assert(size > 0 && "BFBS size must be > 0");

  auto fixed = SchemaMeta<demo::Chat>::FixedSize();
  assert(!fixed.has_value() && "demo::Chat should be variable size");

  auto shm_hint = SchemaMeta<demo::Chat>::ShmCapacityHint();
  assert(!shm_hint.has_value() && "demo::Chat shm capacity hint should be empty");
  return 0;
}
