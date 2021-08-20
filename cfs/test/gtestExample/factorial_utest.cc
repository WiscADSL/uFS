
#include <limits.h>
#include "factorial.h"
#include "gtest/gtest.h"

namespace {
    // Tests factorial of negative numbers.
    TEST(FactorialTest, Negative) {
        // This test is named "Negative", and belongs to the "FactorialTest"
        // test case.
        EXPECT_EQ(1, Factorial(-5));
        EXPECT_EQ(1, Factorial(-1));
        EXPECT_GT(Factorial(-10), 0);

        // <TechnicalDetails>
        //
        // EXPECT_EQ(expected, actual) is the same as
        //
        //   EXPECT_TRUE((expected) == (actual))
        //
        // except that it will print both the expected value and the actual
        // value when the assertion fails.  This is very helpful for
        // debugging.  Therefore in this case EXPECT_EQ is preferred.
        //
        // On the other hand, EXPECT_TRUE accepts any Boolean expression,
        // and is thus more general.
        //
        // </TechnicalDetails>
    }

// Tests factorial of 0.
    TEST(FactorialTest, Zero) {
        EXPECT_EQ(1, Factorial(0));
    }

// Tests factorial of positive numbers.
    TEST(FactorialTest, Positive) {
        EXPECT_EQ(1, Factorial(1));
        EXPECT_EQ(2, Factorial(2));
        EXPECT_EQ(6, Factorial(3));
        EXPECT_EQ(40320, Factorial(8));
    }

} // namespace

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}