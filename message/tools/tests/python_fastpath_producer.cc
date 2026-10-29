#include <fstream>
#include <vector>

#include "message/tools/fastpath_rows_generated.h"

int main(int, char **argv) {
  flatbuffers::FlatBufferBuilder builder;
  std::vector<flatbuffers::Offset<fx_rows::Motor>> motors;
  // Pinned real-arm mock population; CreateMotor supplies flatc's C++ add order.
  for (int i = 0; i < 44; ++i) {
    const bool active = i < 42;
    const std::vector<uint32_t> thermal_values = active
        ? std::vector<uint32_t>{uint32_t(i + 1), uint32_t(i + 2), uint32_t(i + 3)}
        : std::vector<uint32_t>{0, 0, 0};
    const auto thermal = builder.CreateVector(thermal_values);
    motors.push_back(fx_rows::CreateMotor(
        builder, active ? 9997 : 0, active ? 9998 : 0, active ? 9999 : 0,
        active ? 1 : 0, active ? i + .125f : 0, active ? i + .25f : 0,
        active ? i + .5f : 0, active ? i + .75f : 0, active ? i + 1.125f : 0,
        active ? i + 1.25f : 0, active ? i + 1.5f : 0, active ? 40 + i : 0,
        active ? i + 1 : 0, i, active ? fx_rows::Role::ACTIVE : fx_rows::Role::RESERVED,
        active ? 100 + i : 0, thermal, active ? i + 1 : -32768,
        active ? 50 + i : -32768, active ? i + .875f : 0, active ? i + .625f : 0));
  }
  const auto vector = builder.CreateVector(motors);
  builder.Finish(fx_rows::CreateBatch(builder, vector));
  std::ofstream output(argv[1], std::ios::binary);
  output.write(reinterpret_cast<const char *>(builder.GetBufferPointer()), builder.GetSize());
  return output ? 0 : 1;
}
