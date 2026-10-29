#include "fb_reflect.h"

#include <algorithm>
#include <flatbuffers/idl.h>
#include <new>
#include <set>
#include <sstream>
#include <string>
#include <type_traits>
#include <vector>

namespace {
thread_local std::string g_last_error;

void set_error(const std::string& msg) { g_last_error = msg; }

bool deserialize_schema(const uint8_t* bfbs_data,
                        size_t bfbs_size,
                        flatbuffers::Parser* parser) {
  if (!parser->Deserialize(bfbs_data, bfbs_size)) {
    if (parser->error_.empty()) {
      set_error("failed to deserialize BFBS schema");
    } else {
      set_error(parser->error_);
    }
    return false;
  }
  return true;
}

bool apply_root_type(const char* root_type,
                     size_t root_type_size,
                     flatbuffers::Parser* parser) {
  if (root_type == nullptr) {
    if (root_type_size == 0) {
      return true;
    }
    set_error("root_type pointer is null but size is non-zero");
    return false;
  }
  if (root_type_size == 0) {
    set_error("root_type is empty");
    return false;
  }
  const std::string root(root_type, root_type_size);
  if (!parser->SetRootType(root.c_str())) {
    if (parser->error_.empty()) {
      set_error("failed to set root type");
    } else {
      set_error(parser->error_);
    }
    return false;
  }
  return true;
}

bool allocate_c_string(const std::string& text, char** out) {
  char* buf = new (std::nothrow) char[text.size() + 1];
  if (buf == nullptr) {
    set_error("failed to allocate output buffer");
    return false;
  }
  std::copy(text.begin(), text.end(), buf);
  buf[text.size()] = '\0';
  *out = buf;
  return true;
}

std::string join_namespace(const flatbuffers::Namespace* ns) {
  if (ns == nullptr || ns->components.empty()) {
    return "";
  }
  std::ostringstream oss;
  for (size_t i = 0; i < ns->components.size(); ++i) {
    if (i > 0) {
      oss << '.';
    }
    oss << ns->components[i];
  }
  return oss.str();
}

std::string qualified_name(const flatbuffers::Definition& def) {
  const std::string ns = join_namespace(def.defined_namespace);
  if (ns.empty()) {
    return def.name;
  }
  return ns + "." + def.name;
}

std::string format_type(const flatbuffers::Type& type) {
  if (type.base_type == flatbuffers::BASE_TYPE_VECTOR ||
      type.base_type == flatbuffers::BASE_TYPE_VECTOR64) {
    const flatbuffers::Type element(
        type.element, type.struct_def, type.enum_def, type.fixed_length);
    return "[" + format_type(element) + "]";
  }

  if (type.base_type == flatbuffers::BASE_TYPE_ARRAY) {
    const flatbuffers::Type element(type.element, type.struct_def, type.enum_def,
                                    0);
    return "[" + format_type(element) + ":" +
           std::to_string(type.fixed_length) + "]";
  }

  if (type.base_type == flatbuffers::BASE_TYPE_STRUCT &&
      type.struct_def != nullptr) {
    return qualified_name(*type.struct_def);
  }

  if (type.enum_def != nullptr &&
      (type.base_type == flatbuffers::BASE_TYPE_UNION ||
       type.base_type == flatbuffers::BASE_TYPE_UTYPE ||
       flatbuffers::IsInteger(type.base_type))) {
    return qualified_name(*type.enum_def);
  }

  return flatbuffers::TypeName(type.base_type);
}

void collect_struct_dependency(
    const flatbuffers::StructDef* struct_def,
    std::vector<const flatbuffers::StructDef*>* structs,
    std::vector<const flatbuffers::EnumDef*>* enums,
    std::set<const flatbuffers::StructDef*>* seen_structs,
    std::set<const flatbuffers::EnumDef*>* seen_enums);

void collect_type_dependency(
    const flatbuffers::Type& type,
    std::vector<const flatbuffers::StructDef*>* structs,
    std::vector<const flatbuffers::EnumDef*>* enums,
    std::set<const flatbuffers::StructDef*>* seen_structs,
    std::set<const flatbuffers::EnumDef*>* seen_enums) {
  if (type.base_type == flatbuffers::BASE_TYPE_VECTOR ||
      type.base_type == flatbuffers::BASE_TYPE_VECTOR64 ||
      type.base_type == flatbuffers::BASE_TYPE_ARRAY) {
    const flatbuffers::Type element(
        type.element, type.struct_def, type.enum_def, type.fixed_length);
    collect_type_dependency(element, structs, enums, seen_structs, seen_enums);
    return;
  }

  if (type.struct_def != nullptr) {
    collect_struct_dependency(type.struct_def, structs, enums, seen_structs,
                              seen_enums);
  }

  if (type.enum_def != nullptr && seen_enums->insert(type.enum_def).second) {
    enums->push_back(type.enum_def);
  }
}

void collect_struct_dependency(
    const flatbuffers::StructDef* struct_def,
    std::vector<const flatbuffers::StructDef*>* structs,
    std::vector<const flatbuffers::EnumDef*>* enums,
    std::set<const flatbuffers::StructDef*>* seen_structs,
    std::set<const flatbuffers::EnumDef*>* seen_enums) {
  if (!seen_structs->insert(struct_def).second) {
    return;
  }

  structs->push_back(struct_def);
  for (const flatbuffers::FieldDef* field : struct_def->fields.vec) {
    collect_type_dependency(field->value.type, structs, enums, seen_structs,
                            seen_enums);
  }
}

std::string render_struct_block(const flatbuffers::StructDef& struct_def) {
  std::ostringstream oss;
  oss << (struct_def.fixed ? "struct " : "table ")
      << qualified_name(struct_def) << " {\n";

  for (const flatbuffers::FieldDef* field : struct_def.fields.vec) {
    oss << "  " << field->name << ": " << format_type(field->value.type);

    std::vector<std::string> tags;
    if (field->IsRequired()) {
      tags.emplace_back("required");
    } else if (field->IsOptional()) {
      tags.emplace_back("optional");
    }
    if (field->deprecated) {
      tags.emplace_back("deprecated");
    }
    if (field->key) {
      tags.emplace_back("key");
    }

    if (!tags.empty()) {
      oss << " [";
      for (size_t i = 0; i < tags.size(); ++i) {
        if (i > 0) {
          oss << ", ";
        }
        oss << tags[i];
      }
      oss << "]";
    }

    if (field->IsScalar() && !field->value.constant.empty() &&
        field->value.constant != "0") {
      oss << " = " << field->value.constant;
    }

    oss << ";\n";
  }
  oss << "}";
  return oss.str();
}

std::string render_enum_block(const flatbuffers::EnumDef& enum_def) {
  std::ostringstream oss;

  if (enum_def.is_union) {
    oss << "union " << qualified_name(enum_def) << " {\n";
    for (const flatbuffers::EnumVal* value : enum_def.Vals()) {
      if (value == nullptr) {
        continue;
      }
      oss << "  " << value->name;
      if (value->union_type.base_type != flatbuffers::BASE_TYPE_NONE) {
        oss << ": " << format_type(value->union_type);
      }
      oss << " = " << enum_def.ToString(*value) << ";\n";
    }
    oss << "}";
    return oss.str();
  }

  oss << "enum " << qualified_name(enum_def) << " : "
      << flatbuffers::TypeName(enum_def.underlying_type.base_type) << " {\n";
  for (const flatbuffers::EnumVal* value : enum_def.Vals()) {
    if (value == nullptr) {
      continue;
    }
    oss << "  " << value->name << " = " << enum_def.ToString(*value) << ";\n";
  }
  oss << "}";
  return oss.str();
}

bool render_schema_text(flatbuffers::Parser* parser, std::string* out) {
  if (parser->root_struct_def_ == nullptr) {
    set_error("BFBS schema has no root type");
    return false;
  }

  std::vector<const flatbuffers::StructDef*> structs;
  std::vector<const flatbuffers::EnumDef*> enums;
  std::set<const flatbuffers::StructDef*> seen_structs;
  std::set<const flatbuffers::EnumDef*> seen_enums;
  collect_struct_dependency(parser->root_struct_def_, &structs, &enums,
                            &seen_structs, &seen_enums);

  std::ostringstream oss;
  oss << "Root type: " << qualified_name(*parser->root_struct_def_) << "\n\n";

  for (size_t i = 0; i < structs.size(); ++i) {
    if (i > 0) {
      oss << "\n\n";
    }
    oss << render_struct_block(*structs[i]);
  }

  if (!enums.empty()) {
    oss << "\n\n";
    for (size_t i = 0; i < enums.size(); ++i) {
      if (i > 0) {
        oss << "\n\n";
      }
      oss << render_enum_block(*enums[i]);
    }
  }

  *out = oss.str();
  return true;
}

// ── Display renderer ─────────────────────────────────────────────────────────
// Directly walks the binary flatbuffers table using the IDL schema, emitting
// enum identifiers without quotes. Union payload types are resolved from the
// binary discriminant stored in the sibling *_type field, so enum fields inside
// union payloads are also unquoted — the gap that GenerateText+post-processing
// could not address.

struct DisplayPrinter {
  std::string text;
  static constexpr int kIndentStep = 2;

  void AddNewLine() { text += '\n'; }
  void AddIndent(int n) { text.append(static_cast<size_t>(std::max(n, 0)), ' '); }

  // Emit a scalar value. Enum identifiers are written without quotes.
  template <typename T>
  void PrintScalar(T val, const flatbuffers::Type& type) {
    if (flatbuffers::IsBool(type.base_type)) {
      text += val != 0 ? "true" : "false";
      return;
    }
    if (type.enum_def != nullptr &&
        (flatbuffers::IsInteger(type.base_type) ||
         type.base_type == flatbuffers::BASE_TYPE_UTYPE)) {
      // For bit_flags enums, decompose by bits so combined values (e.g.
      // Read|Write = 3) expand to named flags. Only emit names when every
      // set bit is covered by a known enum value — otherwise unknown bits
      // would be silently dropped, which is misleading in forward-compat
      // scenarios. Matches FlatBuffers' upstream idl_gen_text.cpp behavior:
      // fall through to NumToString when coverage is incomplete.
      if (type.enum_def->attributes.Lookup("bit_flags") && val != 0) {
        const auto entry_len = text.size();
        const auto u64 = static_cast<uint64_t>(val);
        uint64_t covered = 0;
        bool first = true;
        for (const flatbuffers::EnumVal* bv : type.enum_def->Vals()) {
          const uint64_t flag = bv->GetAsUInt64();
          if (flag != 0 && (u64 & flag) == flag) {
            if (!first) text += ' ';
            text += bv->name;
            covered |= flag;
            first = false;
          }
        }
        if (!first && covered == u64) return;
        text.resize(entry_len);
      }
      const flatbuffers::EnumVal* ev =
          type.enum_def->ReverseLookup(static_cast<int64_t>(val));
      if (ev != nullptr) {
        text += ev->name;
        return;
      }
    }
    text += flatbuffers::NumToString(val);
  }

  template <typename T>
  static T GetDefault(const flatbuffers::FieldDef& fd) {
    T val{};
    flatbuffers::StringToNumber(fd.value.constant.c_str(), &val);
    return val;
  }

  template <typename T>
  void GenField(const flatbuffers::FieldDef& fd,
                const flatbuffers::Table* table, bool fixed) {
    if (fixed) {
      PrintScalar(
          reinterpret_cast<const flatbuffers::Struct*>(table)->GetField<T>(
              fd.value.offset),
          fd.value.type);
    } else if (fd.IsOptional()) {
      const auto opt = table->GetOptional<T, T>(fd.value.offset);
      if (opt) {
        PrintScalar(*opt, fd.value.type);
      } else {
        text += "null";
      }
    } else {
      PrintScalar(
          table->GetField<T>(fd.value.offset, GetDefault<T>(fd)),
          fd.value.type);
    }
  }

  // ── Container printers ───────────────────────────────────────────────────

  template <typename Container, typename SizeT>
  const char* PrintContainerScalars(const Container& c, SizeT size,
                                     const flatbuffers::Type& elem, int indent) {
    const int inner = indent + kIndentStep;
    text += '[';
    for (SizeT i = 0; i < size; ++i) {
      if (i) text += ',';
      AddNewLine();
      AddIndent(inner);
      PrintScalar(c[i], elem);
    }
    if (size > 0) {
      AddNewLine();
      AddIndent(indent);
    }
    text += ']';
    return nullptr;
  }

  template <typename Container, typename SizeT>
  const char* PrintContainerPtrs(const Container& c, SizeT size,
                                   const flatbuffers::Type& elem,
                                   const uint8_t* prev_val, int indent) {
    const bool is_struct = flatbuffers::IsStruct(elem);
    const int inner = indent + kIndentStep;
    text += '[';
    for (SizeT i = 0; i < size; ++i) {
      if (i) text += ',';
      AddNewLine();
      AddIndent(inner);
      const void* ptr;
      if (is_struct) {
        ptr = reinterpret_cast<const void*>(
            reinterpret_cast<const uint8_t*>(c.Data()) +
            elem.struct_def->bytesize * i);
      } else {
        ptr = static_cast<const void*>(c[i]);
      }
      const char* e = PrintOffset(ptr, elem, prev_val,
                                   static_cast<flatbuffers::soffset_t>(i), inner);
      if (e) return e;
    }
    if (size > 0) {
      AddNewLine();
      AddIndent(indent);
    }
    text += ']';
    return nullptr;
  }

  template <typename T, typename SizeT = flatbuffers::uoffset_t>
  const char* PrintVec(const void* val, const flatbuffers::Type& elem,
                        const uint8_t* prev_val, int indent) {
    using Container = flatbuffers::Vector<T, SizeT>;
    using RetT = typename Container::return_type;
    const auto& vec = *reinterpret_cast<const Container*>(val);
    if constexpr (std::is_pointer<RetT>::value) {
      return PrintContainerPtrs<Container, SizeT>(vec, vec.size(), elem,
                                                   prev_val, indent);
    } else {
      return PrintContainerScalars<Container, SizeT>(vec, vec.size(), elem,
                                                      indent);
    }
  }

  // Dispatch vector element-type printing with an explicit SizeT (vector
  // length width). `SizeT = uoffset_t` for plain `[T]`, `uoffset64_t` for
  // fields declared with the `(vector64)` attribute (BASE_TYPE_VECTOR64).
  // Without the SizeT split, a `(vector64)` field's 8-byte length is read
  // through a 4-byte `flatbuffers::Vector<T>` and silently mis-decodes
  // length + element addresses.
  template <typename SizeT>
  const char* PrintVecBySize(const void* val, const flatbuffers::Type& vt,
                              const uint8_t* prev_val, int indent) {
    switch (vt.base_type) {
// clang-format off
#define FLATBUFFERS_TD(ENUM, IDLTYPE, CTYPE, ...) \
  case flatbuffers::BASE_TYPE_##ENUM: { \
    const char* e = PrintVec<CTYPE, SizeT>(val, vt, prev_val, indent); \
    if (e) return e; \
    break; \
  }
      FLATBUFFERS_GEN_TYPES_SCALAR(FLATBUFFERS_TD)
#undef FLATBUFFERS_TD
// clang-format on
      case flatbuffers::BASE_TYPE_STRING: {
        const char* e =
            PrintVec<flatbuffers::Offset<flatbuffers::String>, SizeT>(
                val, vt, prev_val, indent);
        if (e) return e;
        break;
      }
      case flatbuffers::BASE_TYPE_STRUCT: {
        const char* e = PrintVec<flatbuffers::Offset<void>, SizeT>(
            val, vt, prev_val, indent);
        if (e) return e;
        break;
      }
      case flatbuffers::BASE_TYPE_UNION: {
        const char* e = PrintVec<flatbuffers::Offset<void>, SizeT>(
            val, vt, prev_val, indent);
        if (e) return e;
        break;
      }
      default:
        break;
    }
    return nullptr;
  }

  // ── Core dispatch ────────────────────────────────────────────────────────

  const char* PrintOffset(const void* val, const flatbuffers::Type& type,
                           const uint8_t* prev_val,
                           flatbuffers::soffset_t vector_index, int indent) {
    switch (type.base_type) {
      case flatbuffers::BASE_TYPE_UNION: {
        // Resolve the concrete variant type from the binary discriminant.
        if (prev_val == nullptr)
          return "corrupt buffer: union has no type tag";
        uint8_t disc = *prev_val;
        if (vector_index >= 0) {
          // Union-vector case: type tags stored in a parallel Vector<uint8_t>.
          const auto* tvec =
              reinterpret_cast<const flatbuffers::Vector<uint8_t>*>(
                  prev_val +
                  flatbuffers::ReadScalar<flatbuffers::uoffset_t>(prev_val));
          disc = tvec->Get(static_cast<flatbuffers::uoffset_t>(vector_index));
        }
        const flatbuffers::EnumVal* ev =
            type.enum_def->ReverseLookup(disc, true);
        if (ev == nullptr) return "unknown union type value";
        return PrintOffset(val, ev->union_type, nullptr, -1, indent);
      }
      case flatbuffers::BASE_TYPE_STRUCT:
        return GenStruct(*type.struct_def,
                         reinterpret_cast<const flatbuffers::Table*>(val),
                         indent);
      case flatbuffers::BASE_TYPE_STRING: {
        const auto* s = reinterpret_cast<const flatbuffers::String*>(val);
        if (!flatbuffers::EscapeString(s->c_str(), s->size(), &text,
                                        false, false))
          return "string contains non-utf8 bytes";
        return nullptr;
      }
      case flatbuffers::BASE_TYPE_VECTOR: {
        const flatbuffers::Type vt = type.VectorType();
        return PrintVecBySize<flatbuffers::uoffset_t>(val, vt, prev_val, indent);
      }
      case flatbuffers::BASE_TYPE_VECTOR64: {
        const flatbuffers::Type vt = type.VectorType();
        return PrintVecBySize<flatbuffers::uoffset64_t>(val, vt, prev_val,
                                                        indent);
      }
      case flatbuffers::BASE_TYPE_ARRAY: {
        const flatbuffers::Type at = type.VectorType();
        switch (at.base_type) {
// clang-format off
#define FLATBUFFERS_TD(ENUM, IDLTYPE, CTYPE, ...) \
  case flatbuffers::BASE_TYPE_##ENUM: { \
    using ArrT = flatbuffers::Array<CTYPE, 0xFFFF>; \
    const auto& arr = *reinterpret_cast<const ArrT*>(val); \
    const char* e = PrintContainerScalars<ArrT, uint16_t>( \
        arr, type.fixed_length, at, indent); \
    if (e) return e; \
    break; \
  }
          FLATBUFFERS_GEN_TYPES_SCALAR(FLATBUFFERS_TD)
#undef FLATBUFFERS_TD
// clang-format on
          case flatbuffers::BASE_TYPE_STRUCT: {
            using ArrT = flatbuffers::Array<flatbuffers::Offset<void>, 0xFFFF>;
            const auto& arr = *reinterpret_cast<const ArrT*>(val);
            const char* e = PrintContainerPtrs<ArrT, uint16_t>(
                arr, type.fixed_length, at, nullptr, indent);
            if (e) return e;
            break;
          }
          default: break;
        }
        return nullptr;
      }
      default:
        return nullptr;
    }
  }

  const char* GenFieldOffset(const flatbuffers::FieldDef& fd,
                              const flatbuffers::Table* table, bool fixed,
                              const uint8_t* prev_val, int indent) {
    const void* val;
    if (fixed) {
      val = reinterpret_cast<const flatbuffers::Struct*>(table)
                ->GetStruct<const void*>(fd.value.offset);
    } else if (flatbuffers::IsStruct(fd.value.type)) {
      val = table->GetStruct<const void*>(fd.value.offset);
    } else if (fd.offset64) {
      // Fields tagged `(vector64)` (BASE_TYPE_VECTOR64) — and any future
      // `(offset64)` strings — store their pointer to the heap object as
      // a uoffset64_t (8 bytes) at the vtable slot. Reading via 32-bit
      // GetPointer truncates to the low 4 bytes; for any buffer ≤ 4 GiB
      // the truncation is silently correct (high 4 bytes are zero), but
      // for larger buffers it points at the wrong address. Dispatch on
      // the schema-side flag rather than only BASE_TYPE_VECTOR64 so the
      // narrow case (string-with-(offset64)) is also covered.
      val = table->GetPointer64<const void*>(fd.value.offset);
    } else {
      val = table->GetPointer<const void*>(fd.value.offset);
    }
    return PrintOffset(val, fd.value.type, prev_val, -1, indent);
  }

  const char* GenStruct(const flatbuffers::StructDef& struct_def,
                         const flatbuffers::Table* table, int indent) {
    text += '{';
    int fieldout = 0;
    const uint8_t* prev_val = nullptr;
    const int inner = indent + kIndentStep;
    for (const flatbuffers::FieldDef* fdp : struct_def.fields.vec) {
      const flatbuffers::FieldDef& fd = *fdp;
      const bool present =
          struct_def.fixed || table->CheckField(fd.value.offset);
      // A `FlatBufferBuilder` without ForceDefaults elides scalar fields
      // equal to their schema default from the wire, so an absent scalar is
      // ambiguous between "at default" and "never written". Mirror upstream
      // GenerateText's `output_default_scalars_in_json` gate and synthesize
      // absent scalars anyway: GenField emits GetDefault() for the
      // non-optional case and `null` for an absent optional, preserving the
      // set/unset distinction. Non-scalar fields (strings,
      // vectors, tables, unions) have no default and stay omitted when absent.
      const bool output_anyway =
          flatbuffers::IsScalar(fd.value.type.base_type) && !fd.deprecated;
      if (present || output_anyway) {
        if (fieldout++) text += ',';
        AddNewLine();
        AddIndent(inner);
        text += fd.name;
        text += ": ";
        switch (fd.value.type.base_type) {
// clang-format off
#define FLATBUFFERS_TD(ENUM, IDLTYPE, CTYPE, ...) \
  case flatbuffers::BASE_TYPE_##ENUM: { \
    GenField<CTYPE>(fd, table, struct_def.fixed); \
    break; \
  }
          FLATBUFFERS_GEN_TYPES_SCALAR(FLATBUFFERS_TD)
#undef FLATBUFFERS_TD
#define FLATBUFFERS_TD(ENUM, ...) case flatbuffers::BASE_TYPE_##ENUM:
          FLATBUFFERS_GEN_TYPES_POINTER(FLATBUFFERS_TD)
          FLATBUFFERS_GEN_TYPE_ARRAY(FLATBUFFERS_TD)
#undef FLATBUFFERS_TD
// clang-format on
          {
            const char* e =
                GenFieldOffset(fd, table, struct_def.fixed, prev_val, inner);
            if (e) return e;
            break;
          }
        }
        // Track prev_val: used to resolve the union payload type from the
        // sibling *_type discriminant on the next iteration.
        if (struct_def.fixed) {
          prev_val =
              reinterpret_cast<const uint8_t*>(table) + fd.value.offset;
        } else {
          prev_val = table->GetAddressOf(fd.value.offset);
        }
      }
    }
    if (fieldout > 0) {
      AddNewLine();
      AddIndent(indent);
    }
    text += '}';
    return nullptr;
  }
};

static int fb_to_json_impl(const uint8_t* bfbs_data, size_t bfbs_size,
                           const uint8_t* payload_data, size_t payload_size,
                           const char* root_type, size_t root_type_size,
                           char** out_json) {
  if (out_json == nullptr) {
    set_error("out_json is null");
    return 1;
  }
  *out_json = nullptr;

  if (bfbs_data == nullptr || bfbs_size == 0) {
    set_error("bfbs_data is empty");
    return 1;
  }
  if (payload_data == nullptr) {
    set_error("payload_data is null");
    return 1;
  }
  if (payload_size == 0) {
    set_error("payload_data is empty");
    return 1;
  }

  flatbuffers::Parser parser;
  if (!deserialize_schema(bfbs_data, bfbs_size, &parser)) {
    return 1;
  }
  if (!apply_root_type(root_type, root_type_size, &parser)) {
    return 1;
  }

  // Surface scalar fields whose stored value equals the schema default —
  // FlatBuffers elides them from the buffer, so without this a field at its
  // default reads as "never written" in decoded JSON. Keeps the
  // strict path consistent with the DisplayPrinter above.
  parser.opts.output_default_scalars_in_json = true;
  parser.opts.strict_json = true;

  std::string json;
  const char* generate_err = flatbuffers::GenerateText(
      parser, reinterpret_cast<const void*>(payload_data), &json);
  if (generate_err != nullptr) {
    set_error(generate_err);
    return 1;
  }

  if (!allocate_c_string(json, out_json)) {
    return 1;
  }
  g_last_error.clear();
  return 0;
}
}  // namespace

extern "C" int aorta_introspect_fb_to_json(const uint8_t* bfbs_data,
                                            size_t bfbs_size,
                                            const uint8_t* payload_data,
                                            size_t payload_size,
                                            const char* root_type,
                                            size_t root_type_size,
                                            char** out_json) {
  return fb_to_json_impl(bfbs_data, bfbs_size, payload_data, payload_size,
                         root_type, root_type_size, out_json);
}

extern "C" int aorta_introspect_fb_to_json_display(const uint8_t* bfbs_data,
                                                    size_t bfbs_size,
                                                    const uint8_t* payload_data,
                                                    size_t payload_size,
                                                    const char* root_type,
                                                    size_t root_type_size,
                                                    char** out_json) {
  if (out_json == nullptr) {
    set_error("out_json is null");
    return 1;
  }
  *out_json = nullptr;

  if (bfbs_data == nullptr || bfbs_size == 0) {
    set_error("bfbs_data is empty");
    return 1;
  }
  if (payload_data == nullptr) {
    set_error("payload_data is null");
    return 1;
  }
  if (payload_size == 0) {
    set_error("payload_data is empty");
    return 1;
  }

  flatbuffers::Parser parser;
  if (!deserialize_schema(bfbs_data, bfbs_size, &parser)) return 1;
  if (!apply_root_type(root_type, root_type_size, &parser)) return 1;
  if (parser.root_struct_def_ == nullptr) {
    set_error("BFBS schema has no root type");
    return 1;
  }

  const flatbuffers::Table* root_table = flatbuffers::GetRoot<flatbuffers::Table>(
      reinterpret_cast<const void*>(payload_data));

  DisplayPrinter printer;
  const char* err = printer.GenStruct(*parser.root_struct_def_, root_table, 0);
  if (err != nullptr) {
    set_error(err);
    return 1;
  }
  printer.AddNewLine();

  if (!allocate_c_string(printer.text, out_json)) return 1;
  g_last_error.clear();
  return 0;
}

extern "C" int aorta_introspect_bfbs_to_schema_text(const uint8_t* bfbs_data,
                                                     size_t bfbs_size,
                                                     const char* root_type,
                                                     size_t root_type_size,
                                                     char** out_text) {
  if (out_text == nullptr) {
    set_error("out_text is null");
    return 1;
  }
  *out_text = nullptr;

  if (bfbs_data == nullptr || bfbs_size == 0) {
    set_error("bfbs_data is empty");
    return 1;
  }

  flatbuffers::Parser parser;
  if (!deserialize_schema(bfbs_data, bfbs_size, &parser)) {
    return 1;
  }
  if (!apply_root_type(root_type, root_type_size, &parser)) {
    return 1;
  }

  std::string schema_text;
  if (!render_schema_text(&parser, &schema_text)) {
    return 1;
  }
  if (!allocate_c_string(schema_text, out_text)) {
    return 1;
  }

  g_last_error.clear();
  return 0;
}

extern "C" void aorta_introspect_free_string(char* str) { delete[] str; }

extern "C" const char* aorta_introspect_last_error(void) {
  if (g_last_error.empty()) {
    return "";
  }
  return g_last_error.c_str();
}
