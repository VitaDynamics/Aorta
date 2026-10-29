// Copyright (c) 2025-2026 VitaDynamics
// Unit tests for Status and StatusOr classes

#include "aorta/status.h"
#include "aorta/convenience.h"

#include <gtest/gtest.h>

#include <climits>
#include <memory>

namespace aorta {
namespace {

// Test default constructed Status is OK
TEST(StatusTest, DefaultConstructionIsOk) {
  Status status;
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(status.code(), AORTA_OK);
  EXPECT_TRUE(status.message().empty());
}

// Test Status::Ok() factory method
TEST(StatusTest, OkFactoryMethod) {
  Status status = Status::Ok();
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(status.code(), AORTA_OK);
}

// Test error Status construction
TEST(StatusTest, ErrorConstruction) {
  Status status(AORTA_ERR_INVALID_ARGUMENT, "invalid argument test");
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), AORTA_ERR_INVALID_ARGUMENT);
  EXPECT_EQ(status.message(), "invalid argument test");
}

// Test Status with empty error message
TEST(StatusTest, EmptyErrorMessage) {
  Status status(AORTA_ERR_INTERNAL, "");
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), AORTA_ERR_INTERNAL);
  EXPECT_TRUE(status.message().empty());
}

// Test Status with long error message
TEST(StatusTest, LongErrorMessage) {
  std::string long_message(1000, 'x');
  Status status(AORTA_ERR_TIMEOUT, long_message);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), AORTA_ERR_TIMEOUT);
  EXPECT_EQ(status.message(), long_message);
}

// Test Status move semantics
TEST(StatusTest, MoveSemantics) {
  Status original(AORTA_ERR_NOT_FOUND, "not found");
  Status moved = std::move(original);
  EXPECT_FALSE(moved.ok());
  EXPECT_EQ(moved.code(), AORTA_ERR_NOT_FOUND);
  EXPECT_EQ(moved.message(), "not found");
}

// Test Status copy semantics
TEST(StatusTest, CopySemantics) {
  Status original(AORTA_ERR_BUSY, "busy");
  Status copied = original;
  EXPECT_FALSE(copied.ok());
  EXPECT_EQ(copied.code(), AORTA_ERR_BUSY);
  EXPECT_EQ(copied.message(), "busy");
  // Original should remain unchanged
  EXPECT_FALSE(original.ok());
  EXPECT_EQ(original.code(), AORTA_ERR_BUSY);
}

// Test all error codes
TEST(StatusTest, AllErrorCodes) {
  const int error_codes[] = {
      AORTA_OK,
      AORTA_ERR_INVALID_ARGUMENT,
      AORTA_ERR_NOT_FOUND,
      AORTA_ERR_TIMEOUT,
      AORTA_ERR_BUSY,
      AORTA_ERR_INTERNAL,
      AORTA_ERR_SCHEMA_CONFLICT,
      AORTA_ERR_CONCURRENT_CONFLICT,
  };

  for (int code : error_codes) {
    Status status(code, "test message");
    if (code == AORTA_OK) {
      EXPECT_TRUE(status.ok()) << "Code " << code << " should be OK";
    } else {
      EXPECT_FALSE(status.ok()) << "Code " << code << " should not be OK";
    }
    EXPECT_EQ(status.code(), code);
  }
}

// ==================== StatusOr Tests ====================

// Test StatusOr with error Status
TEST(StatusOrTest, ConstructWithErrorStatus) {
  StatusOr<int> result(Status(AORTA_ERR_INVALID_ARGUMENT, "bad arg"));
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), AORTA_ERR_INVALID_ARGUMENT);
  EXPECT_EQ(result.status().message(), "bad arg");
}

// Test StatusOr with success value
TEST(StatusOrTest, ConstructWithSuccessValue) {
  StatusOr<int> result(42);
  EXPECT_TRUE(result.ok());
  EXPECT_TRUE(result);
  EXPECT_EQ(result.status().code(), AORTA_OK);
  EXPECT_EQ(result.value(), 42);
}

// Test StatusOr with string value
TEST(StatusOrTest, ConstructWithStringValue) {
  StatusOr<std::string> result("hello world");
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.value(), "hello world");
}

// Test StatusOr with shared_ptr value
TEST(StatusOrTest, ConstructWithSharedPtr) {
  auto ptr = std::make_shared<int>(123);
  StatusOr<std::shared_ptr<int>> result(ptr);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(*result.value(), 123);
  EXPECT_EQ(result.value().use_count(), 2);  // ptr + result
}

// Test StatusOr const access
TEST(StatusOrTest, ConstAccess) {
  const StatusOr<int> result(42);
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.value(), 42);
}

TEST(StatusOrTest, DereferenceOperators) {
  StatusOr<std::unique_ptr<int>> result(std::make_unique<int>(42));
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(**result, 42);
  EXPECT_EQ(*result->get(), 42);
}

TEST(StatusOrTest, ValueOrReturnsDefaultOnError) {
  StatusOr<int> error(Status(AORTA_ERR_TIMEOUT, "timeout"));
  EXPECT_EQ(error.value_or(7), 7);
}

TEST(StatusOrTest, ValueOrReturnsStoredValueOnSuccess) {
  StatusOr<std::string> ok(std::string("stored"));
  EXPECT_EQ(ok.value_or("fallback"), "stored");
}

// Test StatusOr move semantics for value
TEST(StatusOrTest, MoveValue) {
  StatusOr<std::unique_ptr<int>> result(std::make_unique<int>(42));
  EXPECT_TRUE(result.ok());
  auto value = std::move(result).value();
  EXPECT_EQ(*value, 42);
}

// Test StatusOr with complex object
TEST(StatusOrTest, ComplexObject) {
  struct Complex {
    int x;
    std::string y;
  };
  StatusOr<Complex> result(Complex{42, "test"});
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.value().x, 42);
  EXPECT_EQ(result.value().y, "test");
}

// Test StatusOr error propagation
TEST(StatusOrTest, ErrorPropagation) {
  Status error_status(AORTA_ERR_TIMEOUT, "operation timed out");
  StatusOr<int> result(error_status);

  EXPECT_FALSE(result.ok());
  const Status& status = result.status();
  EXPECT_EQ(status.code(), AORTA_ERR_TIMEOUT);
  EXPECT_EQ(status.message(), "operation timed out");
}

// Test StatusOr chaining pattern
TEST(StatusOrTest, ChainingPattern) {
  auto get_value = []() -> StatusOr<int> { return 42; };

  auto result = get_value();
  EXPECT_TRUE(result.ok());
  if (result.ok()) {
    EXPECT_EQ(result.value(), 42);
  }
}

// Test StatusOr with void (special case for operations without return value)
TEST(StatusOrTest, VoidSpecializationNotSupported) {
  // StatusOr<void> is not supported by design
  // Users should use Status directly for void operations
  Status status = Status::Ok();
  EXPECT_TRUE(status.ok());
}

TEST(StatusOrDeathTest, ValueAbortsOnErrorStatus) {
  StatusOr<int> result(Status(AORTA_ERR_INTERNAL, "boom"));
  EXPECT_DEATH(static_cast<void>(result.value()), "StatusOr::value\\(\\)");
}

TEST(ConvenienceTest, MustReturnsStoredValue) {
  EXPECT_EQ(Must(StatusOr<int>(42), "answer"), 42);
}

// Must / CheckOk no longer call std::abort() — they
// throw aorta::FatalError so stack RAII unwinds. The two legacy DEATH tests
// (previously matched the abort-time fprintf via EXPECT_DEATH's regex) now
// fail under gtest because its death-test runner installs a top-level
// catch that intercepts the throw before terminate fires. Rewrite as
// EXPECT_THROW assertions; the formatted-message coverage lives in the
// dedicated `Must/CheckOk ThrowsFatalError...` cases above.
TEST(ConvenienceTest, MustLegacyDeathPathNowThrowsFatalError) {
  EXPECT_THROW(static_cast<void>(
                   Must(StatusOr<int>(Status(AORTA_ERR_INTERNAL, "boom")),
                        "create test value")),
               FatalError);
}

TEST(ConvenienceTest, CheckOkAcceptsSuccessStatus) {
  CheckOk(Status::Ok(), "success op");
}

TEST(ConvenienceTest, CheckOkLegacyDeathPathNowThrowsFatalError) {
  EXPECT_THROW(CheckOk(Status(AORTA_ERR_TIMEOUT, "slow"), "wait op"),
               FatalError);
}

// Dispatcher escape hatch: when a user callback invokes Must / CheckOk inside an FFI
// dispatcher (subscriber / service / timer / contextstore / action.*), the
// FatalError throw must NOT be silently swallowed by the dispatcher's
// catch-all (Rust 1.81+ extern "C" is nounwind so we can't re-throw past
// the boundary). Instead the dispatcher catches FatalError specifically and
// terminates the process — strictly better than the former std::abort()
// path because user RAII unwound through the throw before terminate fires.
// This test locks in the convention by simulating the dispatcher pattern.
TEST(ConvenienceDeathTest, FatalErrorEscapeFromSimulatedDispatcherTerminates) {
  EXPECT_DEATH({
    // Simulated dispatcher body — mirrors the structure of
    // cpp/src/{subscriber,service,timer,contextstore}.cc and the two
    // action.h dispatchers (aorta_cpp_on_action_feedback / _on_action_execute).
    try {
      Must(StatusOr<int>(Status(AORTA_ERR_INTERNAL, "boom")),
           "callback inner op");
    } catch (const FatalError& e) {
      std::fprintf(stderr, "Aorta fatal (from test callback): %s\n",
                   e.what());
      std::fflush(stderr);
      std::terminate();
    } catch (...) {
      // The old catch-all path that would have silently swallowed
      // the FatalError. Must come AFTER the FatalError handler.
    }
  }, "Aorta fatal \\(from test callback\\)");
}

// FatalError carries the originating Status and
// `what()` formats both the optional context and the status code/message
// so callers (and RunOrFatal's catch site) can route the failure without
// having to re-derive the diagnostic from stderr.
TEST(ConvenienceTest, FatalErrorWhatIncludesContextAndStatus) {
  FatalError err(Status(AORTA_ERR_TIMEOUT, "slow"), "create thing");
  EXPECT_EQ(err.status().code(), AORTA_ERR_TIMEOUT);
  EXPECT_EQ(err.status().message(), "slow");
  std::string what(err.what());
  EXPECT_NE(what.find("create thing failed"), std::string::npos);
  EXPECT_NE(what.find("code="), std::string::npos);
  EXPECT_NE(what.find("slow"), std::string::npos);
}

TEST(ConvenienceTest, FatalErrorWhatHandlesEmptyContext) {
  FatalError err(Status(AORTA_ERR_INTERNAL, "boom"));
  std::string what(err.what());
  EXPECT_NE(what.find("operation failed"), std::string::npos);
  EXPECT_NE(what.find("boom"), std::string::npos);
}

TEST(ConvenienceTest, MustThrowsFatalErrorOnErrorStatus) {
  try {
    Must(StatusOr<int>(Status(AORTA_ERR_INTERNAL, "boom")), "make int");
    FAIL() << "Must should have thrown";
  } catch (const FatalError& e) {
    EXPECT_EQ(e.status().code(), AORTA_ERR_INTERNAL);
    EXPECT_NE(std::string(e.what()).find("make int failed"),
              std::string::npos);
  }
}

TEST(ConvenienceTest, CheckOkThrowsFatalErrorOnErrorStatus) {
  try {
    CheckOk(Status(AORTA_ERR_TIMEOUT, "slow"), "wait op");
    FAIL() << "CheckOk should have thrown";
  } catch (const FatalError& e) {
    EXPECT_EQ(e.status().code(), AORTA_ERR_TIMEOUT);
    EXPECT_NE(std::string(e.what()).find("wait op failed"),
              std::string::npos);
  }
}

TEST(ConvenienceTest, RunOrFatalReturnsZeroForCleanVoidCallable) {
  bool ran = false;
  int rc = RunOrFatal([&] { ran = true; });
  EXPECT_TRUE(ran);
  EXPECT_EQ(rc, 0);
}

TEST(ConvenienceTest, RunOrFatalPropagatesIntReturn) {
  EXPECT_EQ(RunOrFatal([] { return 0; }), 0);
  EXPECT_EQ(RunOrFatal([] { return 7; }), 7);
}

TEST(ConvenienceTest, RunOrFatalCatchesMustThrowAndReturnsExitFailure) {
  int rc = RunOrFatal([] {
    Must(StatusOr<int>(Status(AORTA_ERR_INTERNAL, "boom")), "ctx");
    return 0;  // unreachable
  });
  EXPECT_EQ(rc, EXIT_FAILURE);
}

TEST(ConvenienceTest, RunOrFatalCatchesArbitraryStdException) {
  int rc = RunOrFatal([] { throw std::runtime_error("unrelated"); });
  EXPECT_EQ(rc, EXIT_FAILURE);
}

TEST(ConvenienceTest, RunOrFatalCatchesNonStdException) {
  int rc = RunOrFatal([] { throw 42; });
  EXPECT_EQ(rc, EXIT_FAILURE);
}

// The core benefit of throw + RunOrFatal is that
// stack RAII fires before main returns. Simulate this with a probe
// object whose destructor records whether unwinding reached it.
TEST(ConvenienceTest, RunOrFatalUnwindsStackBeforeReturning) {
  bool probe_destroyed = false;
  struct Probe {
    bool* flag;
    ~Probe() { *flag = true; }
  };
  int rc = RunOrFatal([&] {
    Probe probe{&probe_destroyed};
    Must(StatusOr<int>(Status(AORTA_ERR_INTERNAL, "boom")));
    // unreachable; if reached the probe would still destroy on scope exit
  });
  EXPECT_EQ(rc, EXIT_FAILURE);
  EXPECT_TRUE(probe_destroyed)
      << "throw path must unwind stack so RAII destructors run";
}

// Test edge cases with status codes
TEST(StatusTest, EdgeCaseStatusCodes) {
  // Test boundary values
  Status status1(0, "zero code");
  EXPECT_TRUE(status1.ok());

  Status status2(-1, "negative one");
  EXPECT_FALSE(status2.ok());

  Status status3(INT_MIN, "min int");
  EXPECT_FALSE(status3.ok());
}

// Test Status message with special characters
TEST(StatusTest, SpecialCharactersInMessage) {
  std::string special = "Error: special chars \n\t\"'!@#$%";
  Status status(AORTA_ERR_INTERNAL, special);
  EXPECT_EQ(status.message(), special);
}

}  // namespace
}  // namespace aorta
