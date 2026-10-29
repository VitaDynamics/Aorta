// Prints `<stem> <SchemaHashHex>` for every schema of the EDU external root.
// Consumed by schema_hash_parity_test.py, which asserts the C++ hash, the
// Python `SCHEMA_HASH_HEX` and sha256(<stem>.bfbs) are the SAME string —
// the "one bfbs is the only schema identity" gate.
#include <cstdio>

#include "edu_demo_job_msg.h"
#include "edu_demo_service_msg.h"
#include "edu_demo_text_msg.h"

int main() {
  std::printf("edu_demo_text %s\n",
              aorta::SchemaMeta<::edu::demo::EduDemoText>::SchemaHashHex());
  std::printf(
      "edu_demo_service %s\n",
      aorta::SchemaMeta<::edu::demo::EduDemoServiceRequest>::SchemaHashHex());
  // An action's aggregate schema keeps the Goal table as its root type (see
  // gen_action_wrapper), so that is the schema-meta identity to compare.
  std::printf("edu_demo_job %s\n",
              aorta::SchemaMeta<::edu::demo::EduDemoJobGoal>::SchemaHashHex());
  return 0;
}
