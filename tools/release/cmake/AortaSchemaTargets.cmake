# Middleware schema headers required by the public SDK; product schemas are separate.
foreach(_schema aorta_base action_common)
  if(NOT TARGET Aorta::schema_${_schema})
    add_library(Aorta::schema_${_schema} INTERFACE IMPORTED)
    set_target_properties(Aorta::schema_${_schema} PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${_AORTA_PREFIX}/include"
      INTERFACE_LINK_LIBRARIES "flatbuffers::flatbuffers")
  endif()
endforeach()
set(Aorta_SCHEMA_TARGETS Aorta::schema_aorta_base Aorta::schema_action_common)
if(NOT TARGET Aorta::schemas)
  add_library(Aorta::schemas INTERFACE IMPORTED)
  set_property(TARGET Aorta::schemas PROPERTY INTERFACE_LINK_LIBRARIES "${Aorta_SCHEMA_TARGETS}")
endif()
unset(_schema)
