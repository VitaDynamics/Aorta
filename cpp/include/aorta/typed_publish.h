#pragma once

#include <type_traits>

namespace aorta {

template <typename T>
struct TypedPublishTraits;

namespace detail {

template <typename T, typename = void>
struct HasTypedPublishTraits : std::false_type {};

template <typename T>
struct HasTypedPublishTraits<T, std::void_t<typename TypedPublishTraits<T>::Msg>>
    : std::true_type {};

}  // namespace detail

}  // namespace aorta
