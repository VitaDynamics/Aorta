// NEGATIVE compile test: old explicit parameter orders.
//
// The pre-freeze explicit `call` constructor took the raw 5-arg shape
// `(client, scope, executor, payload_ptr, payload_len)`; the frozen
// rule is "primary object, plane, payload, scope LAST":
// `call(client, executor, payload_ptr, payload_len, scope)`. The old order
// was DELETED (no deprecated aliases) — this call must fail overload
// resolution (`call` is not a template, so this is a plain constructor
// mismatch, not a deduction-guide rejection).
//
// EXPECTED: fails to compile — "no matching function".
#include <aorta/flow.hpp>

#include <array>
#include <cstdint>

namespace flow = aorta::flow;

flow::Task<void> misuse(uint64_t client, uint64_t executor,
                        flow::Scope scope) {
  std::array<uint8_t, 1> payload{0};
  // OLD order: scope second, raw 5-arg ctor. The frozen signature is
  // (client, executor, payload.data(), payload.size(), scope).
  auto v = co_await flow::call{client, scope, executor, payload.data(),
                               payload.size()};
  (void)v;
}

int main() { return 0; }
