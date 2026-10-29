// Prints `<stem> <schema_hash>` for every schema of the EDU external root.
// Consumed by schema_hash_parity_test.py, which asserts the Rust hash, the C++
// `SchemaHashHex()`, the Python `SCHEMA_HASH_HEX` and sha256(<stem>.bfbs) are
// the SAME string — the "one bfbs is the only schema identity" gate,
// now four-way.
use aorta_sdk::prelude::*;

use edu_msgs::edu_demo_job_msg::schema::EduDemoJobGoalSchema;
use edu_msgs::edu_demo_service_msg::schema::EduDemoServiceRequestSchema;
use edu_msgs::edu_demo_text_msg::schema::EduDemoTextSchema;

fn main() {
    println!(
        "edu_demo_text {}",
        EduDemoTextSchema::schema_hash().to_hex()
    );
    println!(
        "edu_demo_service {}",
        EduDemoServiceRequestSchema::schema_hash().to_hex()
    );
    // An action's aggregate schema keeps the Goal table as its root type (see
    // gen_action_wrapper), so that is the schema-meta identity to compare.
    println!(
        "edu_demo_job {}",
        EduDemoJobGoalSchema::schema_hash().to_hex()
    );
}
