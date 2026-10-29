#include <cmath>
#include <cstdlib>
#include <fstream>
#include <iterator>
#include <limits>
#include <vector>

#include "message/tools/tests/writer_cmd_generated.h"

int main() {
  std::ifstream input(std::getenv("WRITER_BUFFER"), std::ios::binary);
  const std::vector<uint8_t> data((std::istreambuf_iterator<char>(input)), {});
  flatbuffers::Verifier verifier(data.data(), data.size());
  if (!fx_write::VerifyLowCmdBuffer(verifier)) return 1;
  const auto *root = fx_write::GetLowCmd(data.data());
  if (root->stamp() != 123 || root->motor_cmd()->size() != 42) return 1;
  for (int i = 0; i < 42; ++i) {
    const auto *motor = root->motor_cmd()->Get(i);
    if (motor->mode() != i || motor->q() != i + .125f ||
        motor->dq() != i + .25f || motor->tau() != i + .5f ||
        motor->kp() != i + .75f || motor->kd() != i + .875f) return 1;
  }
  if (root->enums()->size() != 2 || root->scalars()->size() != 1) return 1;
  const auto *command = root->enums()->Get(0);
  if (static_cast<int8_t>(command->mode()) != 127 ||
      command->value() != 0 || !std::signbit(command->value()) ||
      command->counter() != std::numeric_limits<uint64_t>::max()) return 1;
  command = root->enums()->Get(1);
  if (static_cast<int8_t>(command->mode()) != -128 ||
      command->value() != std::numeric_limits<float>::infinity() ||
      command->counter() != 0) return 1;
  const auto *scalar = root->scalars()->Get(0);
  return !(scalar->enabled() && scalar->signed8() == -128 &&
           scalar->unsigned8() == 255 && scalar->signed16() == -32768 &&
           scalar->unsigned16() == 65535 &&
           scalar->signed32() == std::numeric_limits<int32_t>::min() &&
           scalar->unsigned32() == std::numeric_limits<uint32_t>::max() &&
           scalar->signed64() == std::numeric_limits<int64_t>::min() &&
           scalar->unsigned64() == std::numeric_limits<uint64_t>::max() &&
           scalar->real32() == .1f && scalar->real64() == -.125);
}
